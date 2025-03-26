// Copyright (c) [2018] - [2025] Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.jobscheduling.watchdog
import java.util.UUID

import akka.Done
import akka.stream.scaladsl.{ Flow, Keep, Sink }
import software.amazon.awssdk.services.ecs.model.{ DescribeTasksResponse, StopTaskRequest, Task }
import com.pennsieve.jobscheduling.db.TaskId
import com.pennsieve.jobscheduling.db.TaskId._
import com.pennsieve.jobscheduling.errors.TaskMissingArnException
import com.pennsieve.jobscheduling.model.JobConverters.{ ImportId, PayloadId }
import com.pennsieve.jobscheduling.model.Tick.Tick
import com.pennsieve.jobscheduling.{ ETLLogContext, WatchDogConfig }
import com.pennsieve.models.JobId
import com.pennsieve.service.utilities.ContextLogger
import io.circe.syntax.EncoderOps

import scala.concurrent.{ ExecutionContext, Future }
import scala.util.control.NonFatal
import scala.util.{ Failure, Success, Try }
import WatchDog.watchDogTier

/**
  * TaskWatchDog checks ECS for orphaned stale tasks and stops them iff they do not already
  * exist in the database
  */
object TaskWatchDog {
  import WatchDog._

  private def logAndReturnEmpty(
    e: Throwable
  )(implicit
    log: ContextLogger
  ): List[Either[NoJobIdException, TaskJobId]] = {
    log.tierNoContext.warn("TaskWatchDog describe tasks failed with", e)
    List.empty
  }

  private def getIds(
    config: WatchDogConfig
  )(
    describeTasksResult: DescribeTasksResponse
  ): List[Either[Exception, TaskJobId]] = {
    def getJobIdAndMaybePayloadId(task: Task, strings: List[String]) =
      strings.map(string => (Try(JobId(UUID.fromString(string))), string)) match {
        case (Success(jobId), _) :: (Failure(_), string) :: Nil =>
          Success((jobId, Try(string.toInt).toOption))

        case (Failure(_), string) :: (Success(jobId), _) :: Nil =>
          Success((jobId, Try(string.toInt).toOption))

        case (Success(jobId), _) :: Nil => Success(jobId, None)

        case _ => Failure(NoJobIdException(s"Could not find valid UUID in $strings"))
      }

    getActiveTasks(config, describeTasksResult)
      .map { task =>
        maybeEnvironmentIds(
          task,
          name => name == ImportId || name == PayloadId,
          list => getJobIdAndMaybePayloadId(task, list)
        ).flatMap {
          case (jobId, payloadId) =>
            TaskId.fromTask(task) match {
              case None => Left(TaskMissingArnException(jobId))
              case Some(taskId) =>
                Right((task, TaskId(task.taskArn, task.clusterArn), jobId, payloadId))
            }
        }
      }
  }

  private def getUntrackedTasksFromECS(
    config: WatchDogConfig,
    clusterArn: String
  )(implicit
    log: ContextLogger,
    ports: WatchDogPorts,
    ec: ExecutionContext
  ): Flow[Tick, WatchDogException, _] =
    Flow[Tick]
      .mapAsync(1)(_ => describeTasks(clusterArn).map(_.fold(logAndReturnEmpty, getIds(config))))
      .mapConcat(x => x)
      .throttle(config.noJobThrottle.parallelism, config.noJobThrottle.period)
      .mapAsyncUnordered(config.noJobThrottle.parallelism) {
        case Left(_) => Future.successful(None)
        case Right((task, taskId, jobId, payloadId)) =>
          // checks if job is in database if it exists it leaves it to the JobWatchDog
          // stream to stop the task
          // which prevents a job from being stopped multiple times
          ports
            .getJob(jobId)
            .map {
              case Some(_) =>
                log.tierContext.info(
                  s"TaskWatchDog found stale task ${taskId.asJson.noSpaces} in ECS but was already in db leaving for JobWatchDog"
                )(ETLLogContext(Some(jobId)))
                None
              case None =>
                log.tierContext.info(
                  s"TaskWatchDog found stale task ${taskId.asJson.noSpaces} in ECS"
                )(ETLLogContext(Some(jobId)))
                Some((task, taskId, jobId, payloadId))
            }
            .recover {
              case NonFatal(e) =>
                log.tierContext.warn(
                  s"TaskWatchDog due to db error found stale task ${taskId.asJson.noSpaces} in ECS",
                  e
                )(ETLLogContext(Some(jobId)))
                Some((task, taskId, jobId, payloadId))
            }
      }
      .collect {
        case Some(taskJob) => taskJob
      }
      .filter {
        case (task, taskId, jobId, _) if isTaskStopped(task) => true
        case (_, _, jobId, _) =>
          log.tierContext.warn(
            s"TaskWatchDog did not stop task for stale job ${jobId} because it looks like it's still running."
          )(ETLLogContext(Some(jobId)))
          false
      }
      .mapAsyncUnordered(config.noJobThrottle.parallelism) {
        case (task, taskId, jobId, payloadId) =>
          ports
            .stopTask(
              StopTaskRequest
                .builder()
                .cluster(taskId.clusterArn)
                .task(taskId.taskArn)
                .build()
            )
            .map {
              _.fold(
                error => FailedToStopTaskException(jobId, payloadId, Some(taskId), error),
                _ =>
                  StoppedTaskWithoutJobException(
                    jobId,
                    payloadId,
                    Some(taskId),
                    NoJobIdException("Could not find job for stopped task")
                  )
              )
            }
      }

  def apply(
    config: WatchDogConfig,
    clusterArn: String
  )(implicit
    log: ContextLogger,
    ports: WatchDogPorts,
    ec: ExecutionContext
  ): Sink[Tick, Future[Done]] =
    getUntrackedTasksFromECS(config, clusterArn).toMat(loggingSink)(Keep.right)
}
