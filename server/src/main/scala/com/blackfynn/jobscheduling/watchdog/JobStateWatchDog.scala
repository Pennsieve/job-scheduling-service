// Copyright (c) [2018] - [2021] Pennsieve, Inc. All Rights Reserved.

package com.blackfynn.jobscheduling.watchdog
import java.time.OffsetDateTime
import java.time.ZoneOffset.UTC

import akka.Done
import akka.stream.alpakka.sqs.ApproximateNumberOfMessages
import akka.stream.scaladsl.{ Flow, Keep, Sink }
import cats.data.EitherT
import cats.implicits._
import com.blackfynn.jobscheduling.JobSchedulingPorts.{ createGetPayload, GetPayload }
import com.blackfynn.jobscheduling.clients.SQSClient.{
  createGetNumberOfMessages,
  createSendMessage,
  GetNumberOfMessages,
  QueueName
}
import com.blackfynn.jobscheduling.commons.JobState
import com.blackfynn.jobscheduling.db.JobsMapper.getJobsByStateAndTimeFilteredByRetries
import com.blackfynn.jobscheduling.db._
import com.blackfynn.jobscheduling.db.profile.api._
import com.blackfynn.jobscheduling.errors.NoPayloadForJob
import com.blackfynn.jobscheduling.handlers.JobsHandlerPorts.NotifyUpload
import com.blackfynn.jobscheduling.model.Tick.Tick
import com.blackfynn.jobscheduling.shapes.EitherPartition.EitherPartitionFlowOps
import com.blackfynn.jobscheduling.watchdog.JobStateWatchDogPorts.GetJobsStuckInState
import com.blackfynn.jobscheduling.{ JobSchedulingPorts, JobStateWatchDogConfig }
import com.blackfynn.service.utilities.ContextLogger

import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.control.NonFatal
import scala.util.{ Failure, Success, Try }
import WatchDog.watchDogTier

object JobStateWatchDog {
  def apply(
    config: JobStateWatchDogConfig,
    ports: JobStateWatchDogPorts
  )(implicit
    ec: ExecutionContext,
    log: ContextLogger
  ): Sink[Tick, Future[Done]] =
    Flow[Tick]
      .mapAsync(1) { _ =>
        getStuckUploadingJobs(config, ports).value
      }
      .collectRightHandleLeftWith { error =>
        log.tierNoContext.error("JobStateWatchDog was unable to process jobs with", error)
      }
      .mapConcat(_.toList)
      .throttle(config.throttle.parallelism, config.throttle.period)
      .mapAsyncUnordered(config.throttle.parallelism) { job =>
        log.tierContext.info("Attempting to resend")(job.logContext)

        resubmitJob(ports, job)
          .leftMap((_, job.logContext))
          .map(_ => job)
          .value
      }
      .toMat {
        Sink.foreach {
          case Left((error, logContext)) =>
            log.tierContext.error(
              "JobStateWatchDog failed to resubmit job to upload consumer",
              error
            )(logContext)
          case Right(job) =>
            log.tierContext.info("JobStateWatchDog resent a job to upload consumers queue")(
              job.logContext
            )
        }
      }(Keep.right)

  private def resubmitJob(
    ports: JobStateWatchDogPorts,
    job: Job
  )(implicit
    ec: ExecutionContext
  ): EitherT[Future, Throwable, Job] =
    for {
      payloadEntry <- EitherT[Future, Throwable, PayloadEntry] {
        ports
          .getPayload(job.payloadId)
          .transform {
            case Success(Some(payloadEntry)) => Success(Right(payloadEntry))
            case Success(None) => Success(Left(NoPayloadForJob))
            case Failure(exception) => Success(Left(exception))
          }
      }
      _ <- JobSchedulingPorts.notifyUploadsConsumer(job, payloadEntry.payload)(
        ports.notifyUpload,
        ec
      )
    } yield job

  private def getStuckUploadingJobs(
    config: JobStateWatchDogConfig,
    ports: JobStateWatchDogPorts
  )(implicit
    ec: ExecutionContext
  ): EitherT[Future, Throwable, Seq[Job]] = {
    def getJobsToSend(
      numberOfMessages: Int,
      jobs: Seq[Job]
    )(implicit
      ec: ExecutionContext
    ): EitherT[Future, Throwable, Seq[Job]] = {
      val space = jobs.length - numberOfMessages

      if (space > 0) EitherT.pure(jobs.take(space))
      else if (jobs.isEmpty) EitherT.pure(jobs)
      else EitherT.leftT(TooManyMessagesStillInQueue)
    }

    for {
      numberOfMessages <- EitherT {
        ports
          .getNumberOfMessages()
          .map {
            _.flatMap {
              _.getAttributes.asScala
                .get(ApproximateNumberOfMessages.name) match {
                case Some(maybeInt) => Try(maybeInt.toInt).toEither
                case None => Left(NoApproximateNumberOfMessagesAttribute)
              }
            }
          }
      }
      jobs <- EitherT {
        ports
          .getJobsStuckInState(
            OffsetDateTime.now(UTC).minusMinutes(config.minutesStuck),
            JobState.Uploading
          )
          .map(_.asRight)
          .recover {
            case NonFatal(e) => Left(e)
          }
      }
      result <- getJobsToSend(numberOfMessages, jobs)
    } yield result
  }
}

case object NoApproximateNumberOfMessagesAttribute extends Throwable

case object TooManyMessagesStillInQueue extends Throwable

case class JobStateWatchDogPorts(
  notifyUpload: NotifyUpload,
  getJobsStuckInState: GetJobsStuckInState,
  getPayload: GetPayload,
  getNumberOfMessages: GetNumberOfMessages
)

object JobStateWatchDogPorts {
  type GetJobsStuckInState = (OffsetDateTime, JobState with JobState.Retryable) => Future[Seq[Job]]
  def createGetJobsStuckInState(
    db: Database,
    maxRetries: Int
  )(implicit
    ec: ExecutionContext
  ): GetJobsStuckInState =
    (time, state) => db.run(getJobsByStateAndTimeFilteredByRetries(time, state, maxRetries))

  def apply(
    config: JobStateWatchDogConfig,
    queueName: QueueName
  )(implicit
    ports: JobSchedulingPorts,
    ec: ExecutionContext
  ): JobStateWatchDogPorts =
    JobStateWatchDogPorts(
      createSendMessage(ports.sqsClient, queueName),
      createGetJobsStuckInState(ports.db, config.maxRetries),
      createGetPayload(ports.db),
      createGetNumberOfMessages(ports.sqsClient, queueName)
    )
}
