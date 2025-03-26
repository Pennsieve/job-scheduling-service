// Copyright (c) [2018] - [2025] Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.jobscheduling.watchdog
import java.time.OffsetDateTime
import java.time.ZoneOffset.UTC
import java.util.Date
import akka.Done
import akka.actor.{ ActorSystem, Scheduler }
import akka.stream.scaladsl.{ Keep, Sink, Source }
import cats.implicits._
import software.amazon.awssdk.services.ecs.model._
import com.pennsieve.jobscheduling.JobSchedulingPorts.FinalSink
import com.pennsieve.jobscheduling.{ JobSchedulingPorts, WatchDogConfig }
import com.pennsieve.jobscheduling.clients.SQSClient.QueueName
import com.pennsieve.jobscheduling.db.{ Job, TaskId }
import com.pennsieve.jobscheduling.model.EventualResult.EventualResult
import com.pennsieve.jobscheduling.model.JobConverters.JobSchedulingService
import com.pennsieve.jobscheduling.model.Tick.Tick
import com.pennsieve.jobscheduling.model._
import com.pennsieve.jobscheduling.shapes.StreamRetry
import com.pennsieve.models.JobId
import com.pennsieve.service.utilities.{ ContextLogger, Tier }

import scala.jdk.CollectionConverters._
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.Try
import scala.util.control.NonFatal

class WatchDog(
  clusterArn: String,
  etlBucket: String,
  uploadQueueName: QueueName,
  config: WatchDogConfig,
  ports: WatchDogPorts
)(implicit
  ec: ExecutionContext,
  jobSchedulingPorts: JobSchedulingPorts,
  scheduler: Scheduler,
  log: ContextLogger
) {
  implicit private val _ports: WatchDogPorts = ports
  implicit private val _config: WatchDogConfig = config

  private val repeatingTick = Source.tick(config.timer.startAfter, config.timer.runEvery, Tick)

  private def runTaskAndJobStreams[A](
    finalSink: FinalSink,
    source: Source[Tick, A]
  )(implicit
    log: ContextLogger,
    system: ActorSystem
  ): Future[Done] =
    for {
      _ <- source.runWith(JobWatchDog(etlBucket, clusterArn).toMat(finalSink)(Keep.right))
      _ <- source
        .toMat(
          JobStateWatchDog(config.jobState, JobStateWatchDogPorts(config.jobState, uploadQueueName))
        )(Keep.right)
        .run()
      done <- source.toMat(TaskWatchDog(config, clusterArn))(Keep.right).run()
    } yield done

  def run[A](
    finalSink: FinalSink,
    source: Source[Tick, A] = repeatingTick
  )(implicit
    log: ContextLogger,
    system: ActorSystem
  ): Future[Done] =
    StreamRetry(() => runTaskAndJobStreams(finalSink, source), config.retry, "WatchDog")
}

object WatchDog {

  type TaskJob = (Task, TaskId, Job)
  type TaskJobId = (Task, TaskId, JobId, Option[Int])
  type MaybeJob = Either[ErrorWithJob, Job]
  type ErrorWithJob = (Throwable, Job)

  implicit val watchDogTier: Tier[WatchDog] = Tier[WatchDog]

  def isTaskStopped(task: Task) = task.desiredStatus == "STOPPED"

  def getCutoffTime(hoursRunning: Long): OffsetDateTime =
    OffsetDateTime.now(UTC).minusHours(hoursRunning)

  def describeTasks(
    clusterArn: String
  )(implicit
    ports: WatchDogPorts
  ): EventualResult[DescribeTasksResponse] = {
    val request = DescribeTasksRequest.builder().cluster(clusterArn).build()

    ports.describeTasks(request)
  }

  def loggingSink(
    implicit
    log: ContextLogger
  ): Sink[WatchDogException, Future[Done]] =
    Sink.foreach[WatchDogException] { ex =>
      log.noContext.warn(s"WatchDog sink emitted ${ex.getMessage}", ex.getCause)
    }

  def maybeEnvironmentIds[A](
    task: Task,
    check: String => Boolean,
    getResult: List[String] => Try[A]
  ): Either[NoJobIdException, A] =
    Try(task.overrides.containerOverrides)
      .flatMap { containerOverride =>
        Try {
          val envs =
            containerOverride.asScala
              .map(_.environment)
              .flatMap {
                _.asScala
                  .flatMap { envPair =>
                    Try((envPair.name, envPair.value)).toOption
                  }
                  .filter {
                    case (name, _) => check(name)
                  }
              }
              .map { case (_, id) => id }

          getResult(envs.toList).get
        }
      }
      .toEither
      .leftMap {
        case NonFatal(_) => NoJobIdException(task)
      }

  def getActiveTasks(
    config: WatchDogConfig,
    describeTasksResult: DescribeTasksResponse
  ): List[Task] =
    describeTasksResult.tasks.asScala.toList
      .filter { task =>
        val withinTimePeriod =
          Try(task.createdAt)
            .fold(_ => false, date => date.isBefore(getCutoffTime(config.hoursRunning).toInstant))

        val wasStartedByEtlService =
          Try(task.startedBy)
            .fold(_ => false, {
              case JobSchedulingService => true
              case _ => false
            })

        withinTimePeriod && wasStartedByEtlService
      }
}
