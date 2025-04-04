// Copyright (c) [2018] - [2025] Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.jobscheduling.pusher

import akka.NotUsed
import akka.actor.Scheduler
import akka.pattern.after
import akka.stream.scaladsl.Flow
import cats.data.EitherT
import cats.implicits._
import software.amazon.awssdk.services.ecs.model.{ RunTaskRequest, RunTaskResponse }
import com.pennsieve.jobscheduling.commons.JobState
import com.pennsieve.jobscheduling.db.{ Job, TaskId }
import com.pennsieve.jobscheduling.errors.{
  NoPayloadForJob,
  TaskMissingArnException,
  UnsupportedPayload
}
import com.pennsieve.jobscheduling.model.ETLEvent
import com.pennsieve.jobscheduling.model.EventualResult.EventualResult
import com.pennsieve.jobscheduling.model.JobConverters._
import com.pennsieve.jobscheduling.{ ECSConfig, PusherConfig }
import com.pennsieve.models.Manifest
import com.pennsieve.models.PayloadType.{ Append, Export, Workflow }
import com.pennsieve.service.utilities.ContextLogger
import io.circe.syntax.EncoderOps

import scala.jdk.CollectionConverters._
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.control.NonFatal
import com.pennsieve.jobscheduling.scheduler.JobSchedulerPorts.jobSchedulerTier

class JobPusher(
  ports: JobPusherPorts,
  pusherConfig: PusherConfig,
  etlBucket: String,
  ecsConfig: ECSConfig
)(implicit
  log: ContextLogger,
  scheduler: Scheduler,
  executionContext: ExecutionContext
) {

  private val maxAttempts = ecsConfig.task.maxAttempts

  val flow: Flow[Job, ETLEvent, NotUsed] =
    Flow[Job]
      .mapAsync[(Either[Throwable, TaskId], Job)](parallelism = 1) { job =>
        for {
          maybeUnit <- putManifest(job)
          result <- maybeUnit match {
            case Right(_) => startJob(job)
            case Left(error) => Future.successful((Left(error), job))
          }
        } yield result
      }
      .map {
        case (Left(error), job) =>
          (error, job).toTaskCreationFailedEvent(etlBucket)

        case (Right(taskId: TaskId), job) =>
          val context = job.logContext
          log.tierContext.info(s"JobPusher started ecs task for ${job.asJson.noSpaces}")(context)

          job.toSuccessfulEvent(etlBucket, taskId, JobState.Submitted)
      }

  private def startJob(job: Job) = {
    log.tierContext.info(s"JobPusher attempting to start task for ${job.asJson.noSpaces}")(
      job.logContext
    )
    val runTaskRequest =
      job.toRunTaskRequest(etlBucket, pusherConfig, ecsConfig)

    runTask(runTaskRequest, job)
      .map {
        _.map(_.tasks.asScala.toList.headOption) match {
          case Right(Some(task)) =>
            TaskId.fromTask(task) match {
              case None => (Left(TaskMissingArnException(job.id)), job)
              case Some(taskId) => (Right(taskId), job)
            }
          case Right(None) => (Left(NoTaskStarted), job)
          case Left(err) => (Left(err), job)
        }
      }
  }

  private def putManifest(job: Job) = {
    log.tierContext.info(s"JobPusher uploading manifest for ${job.asJson.noSpaces}")(job.logContext)

    def getManifest(job: Job): EventualResult[Manifest] =
      ports
        .getPayload(job.payloadId)
        .map { maybePayloadEntry =>
          for {
            payloadEntry <- Either.fromOption(maybePayloadEntry, NoPayloadForJob)
            payloadType <- {
              payloadEntry.payload.`type` match {
                case Workflow => Workflow.asRight[Exception]
                case Export => Export.asRight[Exception]
                case Append => Append.asRight[Exception]
                case _ => UnsupportedPayload(payloadEntry.payload).asLeft
              }
            }
          } yield {
            Manifest(
              `type` = payloadType,
              importId = job.id,
              organizationId = job.organizationId,
              content = payloadEntry.payload
            )
          }
        }
        .recover {
          case NonFatal(e) => Left(GetPayloadException(e))
        }

    val eventualManifestPut: EitherT[Future, Throwable, Unit] =
      for {
        manifest <- EitherT(getManifest(job))

        result <- EitherT(ports.putManifest(manifest, executionContext))
      } yield result

    eventualManifestPut.value
      .recover {
        case NonFatal(e) => Left(ManifestUploadFailure(e))
      }
  }

  private def runTask(
    task: RunTaskRequest,
    job: Job,
    attempts: Int = 1
  )(implicit
    executionContext: ExecutionContext,
    scheduler: Scheduler
  ): Future[Either[Throwable, RunTaskResponse]] =
    ports
      .runTask(task)
      .flatMap[Either[Throwable, RunTaskResponse]] {
        case Right(runTaskResult) if runTaskResult.failures.isEmpty =>
          Future.successful(Right(runTaskResult))

        case _ if attempts < maxAttempts =>
          log.tierNoContext.info(s"""JobPusher attempting to submit
               |{
               |  "attempts":"$attempts",
               |  "attemptsRemaining":${maxAttempts - attempts},
               |  "task":${task.toString}
              """.stripMargin)
          after((2 * attempts).seconds, scheduler)(runTask(task, job, attempts + 1))

        case Left(e) =>
          log.tierContext.error("JobPusher task rejected", e)(job.logContext)
          Future.successful(Left(JobRejected))

        case Right(taskWithFail) =>
          log.tierContext.error(s"JobPusher RunTaskResult has failures ${taskWithFail.toString}")(
            job.logContext
          )
          Future.successful(Left(JobRejected))
      }
}

object JobPusher {
  type ErrorWithJob = (Throwable, Job)
}

case object JobRejected extends Exception
case class ManifestUploadFailure(cause: Throwable) extends Exception(cause)
case class GetPayloadException(cause: Throwable) extends Exception(cause)
case object NoTaskStarted extends Exception
