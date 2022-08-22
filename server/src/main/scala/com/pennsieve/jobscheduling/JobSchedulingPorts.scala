// Copyright (c) [2018] - [2022] Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.jobscheduling
import java.time.OffsetDateTime
import akka.Done
import akka.stream.scaladsl.Sink
import cats.data.EitherT
import cats.implicits._
import software.amazon.awssdk.services.sqs.model.SendMessageResponse
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import com.pennsieve.auth.middleware.Jwt
import com.pennsieve.jobscheduling.clients.SQSClient.MessageBody
import com.pennsieve.jobscheduling.clients.{ ECSClient, ManifestS3Client }
import com.pennsieve.jobscheduling.commons.JobState
import com.pennsieve.jobscheduling.db.JobsMapper.get
import com.pennsieve.jobscheduling.db.profile.api._
import com.pennsieve.jobscheduling.db._
import com.pennsieve.jobscheduling.errors.{
  NoPayloadException,
  NoTaskException,
  UnsupportedPayload
}
import com.pennsieve.jobscheduling.handlers.JobsHandlerPorts.NotifyUpload
import com.pennsieve.jobscheduling.model.EventualResult.{
  EitherContext,
  EventualResult,
  EventualResultContext
}
import com.pennsieve.jobscheduling.model.JobConverters.payloadToManifest
import com.pennsieve.jobscheduling.model.{ ETLEvent, ManifestUri }
import com.pennsieve.jobscheduling.scheduler.{ JobNotQueued, JobQueued, JobScheduler }
import com.pennsieve.models.{ JobId, Manifest, Payload, Upload }
import com.pennsieve.service.utilities.{ ContextLogger, Tier }
import com.zaxxer.hikari.HikariDataSource
import io.circe.syntax.EncoderOps
import slick.util.AsyncExecutor
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient

import scala.concurrent.{ ExecutionContext, Future }
import scala.util.control.NonFatal
import scala.util.{ Failure, Success }

class JobSchedulingPorts(config: ServiceConfig) {
  val db: Database = {
    val hikariDataSource = new HikariDataSource()

    hikariDataSource.setJdbcUrl(config.postgres.jdbcURL)
    hikariDataSource.setUsername(config.postgres.user)
    hikariDataSource.setPassword(config.postgres.password)
    hikariDataSource.setMaximumPoolSize(config.postgres.maxConnections)
    hikariDataSource.setDriverClassName("org.postgresql.Driver")

    Database.forDataSource(
      hikariDataSource,
      maxConnections = None, // Ignored if an executor is provided
      executor = AsyncExecutor(
        name = "AsyncExecutor.pennsieve",
        minThreads = config.postgres.minThreads,
        maxThreads = config.postgres.maxThreads,
        maxConnections = config.postgres.maxConnections,
        queueSize = config.postgres.queueSize
      )
    )
  }

  val ecsClient: ECSClient = new ECSClient(config.ecs.awsRegion)

  val manifestClient: ManifestS3Client = ManifestS3Client(config.s3)

  val sqsClient: SqsAsyncClient =
    SqsAsyncClient
      .builder()
      .httpClientBuilder(NettyNioAsyncHttpClient.builder())
      .credentialsProvider(DefaultCredentialsProvider.create())
      .region(config.jobMonitor.awsRegion)
      .build()

  val jwt: Jwt.Config = new Jwt.Config {
    val key: String = config.jwt.key
  }
}

object JobSchedulingPorts {
  type GetPayload = Int => Future[Option[PayloadEntry]]
  def createGetPayload(db: Database): GetPayload =
    payloadId => db.run(PayloadsMapper.get(payloadId))

  type GetJob = JobId => Future[Option[Job]]
  def createGetJob(db: Database): GetJob = jobId => db.run(get(jobId))

  type UpdateJob =
    (JobId, JobState, OffsetDateTime, Option[TaskId], ETLLogContext) => EventualResultContext[Unit]
  def createUpdateJob(db: Database)(implicit ec: ExecutionContext): UpdateJob =
    (
      jobId: JobId,
      newState: JobState,
      sentAt: OffsetDateTime,
      taskId: Option[TaskId],
      log: ETLLogContext
    ) => {
      (taskId, newState) match {
        case (Some(id), _) =>
          db.run(JobsMapper.updateState(jobId, newState, id, sentAt))
        case (None, state: JobState.NoTaskId) =>
          db.run(JobsMapper.updateState(jobId, state, sentAt))
        case _ => Future.failed(NoTaskException(jobId))
      }
    }.transform {
      case Success(_) => Success(Right(()))
      case Failure(error) => Success(Left((error, log)))
    }

  type NotifyJobSource = () => Future[Either[JobNotQueued.JobNotQueued, JobQueued.JobQueued]]
  def createNotifyJobSource(scheduler: JobScheduler): NotifyJobSource = scheduler.addJob

  type GetManifest = ManifestUri => EventualResult[Manifest]

  type GenericFinalSink[A] = Sink[EitherContext[ETLEvent], Future[A]]
  type FinalSink = GenericFinalSink[Done]
  def finalSink(
    sender: String
  )(implicit
    log: ContextLogger
  ) =
    Sink.foreach[EitherContext[ETLEvent]] {
      case Right(etlEvent) =>
        log.context.info(s"$sender sent etl event ${etlEvent.asJson.noSpaces}")(
          ETLLogContext(etlEvent)
        )
      case Left((err, logContext)) =>
        log.context.error(s"$sender failed to complete event", err)(logContext)
    }

  def getPayloadFromEvent(
    event: ETLEvent,
    getPayload: GetPayload,
    getJob: GetJob,
    getManifest: GetManifest,
    useDatabase: Boolean = true
  )(implicit
    ec: ExecutionContext
  ): Future[Either[Throwable, Payload]] =
    event.payloadId match {
      case Some(payloadId) if useDatabase =>
        getPayload(payloadId)
          .map {
            _.fold[Either[Throwable, Payload]](Left(NoPayloadException(payloadId))) { entry =>
              Right(entry.payload)
            }
          }
          .recoverWith {
            case NonFatal(_) =>
              getPayloadFromEvent(event, getPayload, getJob, getManifest, useDatabase = false)
          }

      case _ =>
        getManifest(event.manifestUri).map(manifest => manifest.map(_.content))
    }

  def notifyUploadsConsumer(
    job: Job,
    payload: Payload
  )(implicit
    notifyUploadConsumer: NotifyUpload,
    executionContext: ExecutionContext
  ): EitherT[Future, Throwable, SendMessageResponse] = {
    val isValidPayload: Either[Throwable, Unit] = payload match {
      case _: Upload => Right(())
      case unsupportedPayload => Left(UnsupportedPayload(unsupportedPayload))
    }

    for {
      _ <- isValidPayload.toEitherT[Future]
      manifest <- payloadToManifest(job)(Some(payload)).toEitherT[Future]
      manifestString = manifest.asJson.noSpaces
      result <- EitherT(notifyUploadConsumer(MessageBody(manifestString)))
    } yield result
  }
}
