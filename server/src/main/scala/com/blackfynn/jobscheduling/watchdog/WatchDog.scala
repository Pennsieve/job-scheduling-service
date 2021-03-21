// Copyright (c) [2018] - [2021] Pennsieve, Inc. All Rights Reserved.

package com.blackfynn.jobscheduling.watchdog
import java.time.OffsetDateTime
import java.time.ZoneOffset.UTC
import java.util.Date

import akka.Done
import akka.actor.Scheduler
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{ Keep, Sink, Source }
import cats.implicits._
import com.amazonaws.services.ecs.model._
import com.blackfynn.jobscheduling.JobSchedulingPorts.FinalSink
import com.blackfynn.jobscheduling.{ JobSchedulingPorts, WatchDogConfig }
import com.blackfynn.jobscheduling.clients.SQSClient.QueueName
import com.blackfynn.jobscheduling.db.{ Job, TaskId }
import com.blackfynn.jobscheduling.model.EventualResult.EventualResult
import com.blackfynn.jobscheduling.model.JobConverters.JobSchedulingService
import com.blackfynn.jobscheduling.model.Tick.Tick
import com.blackfynn.jobscheduling.model._
import com.blackfynn.jobscheduling.shapes.StreamRetry
import com.blackfynn.models.JobId
import com.blackfynn.service.utilities.{ ContextLogger, Tier }

import scala.collection.JavaConverters._
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
    fm: ActorMaterializer
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
    fm: ActorMaterializer
  ): Future[Done] =
    StreamRetry(() => runTaskAndJobStreams(finalSink, source), config.retry, "WatchDog")
}

object WatchDog {

  type TaskJob = (Task, TaskId, Job)
  type TaskJobId = (Task, TaskId, JobId, Option[Int])
  type MaybeJob = Either[ErrorWithJob, Job]
  type ErrorWithJob = (Throwable, Job)

  implicit val watchDogTier: Tier[WatchDog] = Tier[WatchDog]

  def isTaskStopped(task: Task) = task.getDesiredStatus == "STOPPED"

  def getCutoffTime(hoursRunning: Long): OffsetDateTime =
    OffsetDateTime.now(UTC).minusHours(hoursRunning)

  def describeTasks(
    clusterArn: String
  )(implicit
    ports: WatchDogPorts
  ): EventualResult[DescribeTasksResult] = {
    val request = new DescribeTasksRequest()

    request.setCluster(clusterArn)

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
    Try(task.getOverrides.getContainerOverrides)
      .flatMap { containerOverride =>
        Try {
          val envs =
            containerOverride.asScala
              .map(_.getEnvironment)
              .flatMap {
                _.asScala
                  .flatMap { envPair =>
                    Try((envPair.getName, envPair.getValue)).toOption
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

  def getActiveTasks(config: WatchDogConfig, describeTasksResult: DescribeTasksResult): List[Task] =
    describeTasksResult.getTasks.asScala.toList
      .filter { task =>
        val withinTimePeriod =
          Try(task.getCreatedAt)
            .fold(
              _ => false,
              date =>
                date.before(new Date(getCutoffTime(config.hoursRunning).toInstant.toEpochMilli))
            )

        val wasStartedByEtlService =
          Try(task.getStartedBy)
            .fold(_ => false, {
              case JobSchedulingService => true
              case _ => false
            })

        withinTimePeriod && wasStartedByEtlService
      }
}
