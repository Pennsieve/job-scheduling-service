// Copyright (c) [2018] - [2022] Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.jobscheduling.watchdog

import java.util.UUID

import akka.NotUsed
import akka.stream.FlowShape
import akka.stream.scaladsl.{ Flow, GraphDSL, Merge }
import software.amazon.awssdk.services.ecs.model.{ DescribeTasksResponse, StopTaskRequest }
import com.pennsieve.jobscheduling.WatchDogConfig
import com.pennsieve.jobscheduling.commons.JobState
import com.pennsieve.jobscheduling.db.{ DatabaseClientFlows, Job, TaskId }
import com.pennsieve.jobscheduling.errors.{ NoTaskException, TaskMissingArnException }
import com.pennsieve.jobscheduling.model.EventualResult.EitherContext
import com.pennsieve.jobscheduling.model.JobConverters.ImportId
import com.pennsieve.jobscheduling.model.Tick.Tick
import com.pennsieve.jobscheduling.model._
import com.pennsieve.jobscheduling.shapes.EitherPartition
import com.pennsieve.jobscheduling.watchdog.WatchDog._
import com.pennsieve.models.{ JobId, PackageState }
import com.pennsieve.service.utilities.ContextLogger
import io.circe.syntax.EncoderOps

import scala.concurrent.ExecutionContext
import scala.util.Try
import WatchDog.watchDogTier

/**
  *
  * JobWatchDog checks the database for stale jobs and stops the corresponding task
  *
  */
object JobWatchDog {

  private def updateFlow(
    implicit
    ec: ExecutionContext,
    ports: WatchDogPorts,
    config: WatchDogConfig,
    log: ContextLogger
  ): Flow[ETLEvent, EitherContext[ETLEvent], NotUsed] =
    Flow[ETLEvent]
      .via(
        DatabaseClientFlows
          .updateDatabaseFlow(config.jobThrottle.sink, ports.updateJob, ports.notifyJobSource)
      )
      .via(
        DatabaseClientFlows.getPayloadFlow(
          config.jobThrottle.sink,
          ports.getPayload,
          ports.getJob,
          ports.getManifest
        )
      )
      .via(ports.pennsieveApiClient.updatePackageFlow(config.jobThrottle.sink))
      .map {
        case Right((event, payload)) => Right(event)
        case Left(errorContext) => Left(errorContext)
      }

  private def createGetTask(
    config: WatchDogConfig,
    clusterArn: String
  )(implicit
    ports: WatchDogPorts,
    ec: ExecutionContext,
    log: ContextLogger
  ) = {
    def getJobTask(job: Job)(describeTasksResult: DescribeTasksResponse) =
      getActiveTasks(config, describeTasksResult)
        .map { task =>
          maybeEnvironmentIds(
            task = task,
            check = _ == ImportId,
            getResult = list => Try(JobId(UUID.fromString(list.head)))
          ).flatMap(
            jobId =>
              TaskId.fromTask(task) match {
                case None => Left(TaskMissingArnException(job.id))
                case Some(taskId) => Right((task, taskId, jobId))
              }
          )
        }
        .find {
          case Right((_, _, jobId)) => jobId == job.id
          case left @ Left(_) => true
        }
        .getOrElse(Left(NoTaskException(job.id)))
        .fold(ex => Left((ex, job)), taskJob => Right((taskJob._1, taskJob._2, job)))

    Flow[Job]
      .throttle(config.jobThrottle.ecs.parallelism, config.jobThrottle.ecs.period)
      .mapAsyncUnordered(config.jobThrottle.ecs.parallelism) { job =>
        describeTasks(clusterArn)
          .map {
            _.fold(ex => Left((ex, job)), getJobTask(job))
          }
      }
      .filter {
        case Right((task, _, _)) if isTaskStopped(task) => true
        case Right((_, _, job)) =>
          log.tierContext.warn(
            s"JobWatchDog did not stop task for stale job ${job.id} because it looks like it's still running."
          )(job.logContext)
          false
        case Left(_) => true
      }
  }

  private def createGetOldActiveJobs(
    config: WatchDogConfig
  )(implicit
    ports: WatchDogPorts,
    log: ContextLogger
  ) =
    Flow[Tick]
      .mapAsync(parallelism = 1) { _ =>
        ports.getOldActiveJobs(getCutoffTime(config.hoursRunning))
      }
      .mapConcat(x => x.toList)
      .map { job =>
        log.tierContext.info(s"JobWatchDog found stale job ${job.asJson.noSpaces}")(job.logContext)
        job
      }

  private def stopTasks(
    config: WatchDogConfig,
    etlBucket: String
  )(implicit
    ports: WatchDogPorts,
    ec: ExecutionContext,
    log: ContextLogger
  ) =
    Flow[TaskJob]
      .throttle(config.jobThrottle.ecs.parallelism, config.jobThrottle.ecs.period)
      .mapAsyncUnordered(config.jobThrottle.ecs.parallelism) {
        case (task, taskId, job) =>
          ports
            .stopTask(
              StopTaskRequest
                .builder()
                .cluster(taskId.clusterArn)
                .task(taskId.taskArn)
                .build()
            )
            .map {
              _.fold(error => Left((error, (task, taskId, job))), _ => Right((task, taskId, job)))
            }
      }
      .map {
        case Right((task, taskId, job)) =>
          log.tierContext.info(s"JobWatchDog stopped task ${task.toString}")(job.logContext)

          Right(
            SuccessfulEvent(
              job.id,
              taskId,
              JobState.Failed,
              Some(job.payloadId),
              ManifestUri(etlBucket, job.id),
              job.organizationId,
              job.userId
            )
          )
        case Left((error, (_, _, job))) => Left((error, job))
      }

  def apply(
    etlBucket: String,
    clusterArn: String
  )(implicit
    ec: ExecutionContext,
    ports: WatchDogPorts,
    config: WatchDogConfig,
    log: ContextLogger
  ): Flow[Tick, EitherContext[ETLEvent], _] =
    Flow fromGraph {
      GraphDSL.create() { implicit builder =>
        import GraphDSL.Implicits._
        val mergeForUpdate = builder.add(Merge[ETLEvent](2))
        val getStaleActiveJobs = builder.add(createGetOldActiveJobs(config))
        val getTask = builder.add(createGetTask(config, clusterArn))

        val errorOrEvent = builder.add(EitherPartition[ErrorWithJob, ETLEvent])
        val failedOrFoundTask = builder.add(EitherPartition[ErrorWithJob, TaskJob])

        val update = builder.add(updateFlow)

        val lostTaskOut =
          failedOrFoundTask.out0.map {
            case (error, job) =>
              log.tierContext.error(
                s"JobWatchDog no task could be found for ${job.asJson.noSpaces}",
                error
              )(job.logContext)

              FailedLostTaskEvent(
                job.id,
                ManifestUri(etlBucket, job.id),
                job.organizationId,
                job.userId,
                Some(job.payloadId)
              )
          }
        val failedToStopTaskOut = errorOrEvent.out0.map {
          case (throwable, job) =>
            log.tierContext.error(
              s"JobWatchDog failed to stop task for ${job.asJson.noSpaces}",
              throwable
            )(job.logContext)
            FailedToStopTaskException(job.id, Some(job.payloadId), job.taskId, throwable)
        }
        val stoppedTaskEventOut = errorOrEvent.out1

        getStaleActiveJobs ~> getTask ~> failedOrFoundTask.in

        failedOrFoundTask.out1 ~> stopTasks(config, etlBucket) ~> errorOrEvent.in

        lostTaskOut ~> mergeForUpdate
        stoppedTaskEventOut ~> mergeForUpdate

        failedToStopTaskOut ~> loggingSink

        mergeForUpdate ~> update.in

        FlowShape(getStaleActiveJobs.in, update.out)
      }
    }

}
