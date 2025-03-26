// Copyright (c) [2018] - [2025] Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.jobscheduling.monitor

import java.net.URLDecoder
import java.nio.charset.StandardCharsets.UTF_8
import java.time.OffsetDateTime
import java.util.UUID

import cats.implicits._
import com.pennsieve.jobscheduling.ETLLogContext
import com.pennsieve.jobscheduling.clients.SQSClient.ReceiptHandle
import com.pennsieve.jobscheduling.monitor.CloudwatchMessageDecoders.{
  runningTaskEventDecoder,
  stoppedTaskEventDecoder
}
import com.pennsieve.jobscheduling.commons.JobState
import com.pennsieve.jobscheduling.db.{ Job, TaskId }
import com.pennsieve.jobscheduling.errors.NoJobException
import com.pennsieve.jobscheduling.model.JobConverters.{ ImportId, RichJob }
import com.pennsieve.jobscheduling.model.{ ETLEvent, ManifestUri }
import com.pennsieve.models.JobId
import io.circe.generic.semiauto._
import io.circe.{ Decoder, DecodingFailure, Encoder, HCursor }

import scala.concurrent.{ ExecutionContext, Future }
import scala.util.Try

case class AWSEnvironmentVariable(name: String, value: String)

object AWSEnvironmentVariable {
  implicit val decoder: Decoder[AWSEnvironmentVariable] = deriveDecoder[AWSEnvironmentVariable]
}

/**
  * Cloudwatch messages emitted by Nextflow tasks running in ECS
  *
  * See https://github.com/Pennsieve/etl-infrastructure/blob/68d55e6d033637ea6d35aa39ef3f73899379a5f1/cloudwatch.tf#L30-L53
  */
case class CloudwatchMessage(
  importId: JobId,
  taskId: TaskId,
  jobState: JobState,
  manifestUri: ManifestUri,
  sentAt: OffsetDateTime
) {

  def toEtlEvent(
    etlBucket: String,
    receiptHandle: ReceiptHandle,
    jobIdToJob: JobId => Future[Option[Job]]
  )(implicit
    ec: ExecutionContext
  ): Future[Either[Throwable, ETLEvent]] =
    jobIdToJob(this.importId)
      .map {
        case Some(job) =>
          job.taskId match {
            case Some(jobTaskId) =>
              job
                .toSuccessfulEvent(etlBucket, jobTaskId, this.jobState, Some(receiptHandle), sentAt)
                .asRight
            case None =>
              job
                .toSuccessfulEvent(etlBucket, taskId, this.jobState, Some(receiptHandle), sentAt)
                .asRight
          }

        case None => Left(NoJobException(this.importId))
      }

  def getLogContext: ETLLogContext = ETLLogContext(Some(this.importId))
}

object CloudwatchMessage {
  import JobId._
  import com.pennsieve.jobscheduling.model.OffsetDateTimeEncoder._
  implicit val encoder: Encoder[CloudwatchMessage] = deriveEncoder[CloudwatchMessage]
  implicit val decoder: Decoder[CloudwatchMessage] =
    runningTaskEventDecoder.or(stoppedTaskEventDecoder)
}

object CloudwatchMessageDecoders {
  implicit class EnvironmentMap(l: List[AWSEnvironmentVariable]) {
    def asMap: Map[String, String] = l.foldLeft(Map.empty[String, String]) {
      case (m, e) =>
        m + (e.name -> e.value)
    }
  }

  import AWSEnvironmentVariable._

  val stoppedTaskEventDecoder: Decoder[CloudwatchMessage] =
    (c: HCursor) => {
      val detail = c.downField("detail")

      for {
        taskArn <- detail.get[String]("taskArn")
        clusterArn <- detail.get[String]("clusterArn")

        exitCode <- detail
          .downField("containers")
          .downArray
          .downField("exitCode")
          .as[Option[Int]]

        _environment <- detail
          .downField("overrides")
          .downField("containerOverrides")
          .downArray
          .downField("environment")
          .as[List[AWSEnvironmentVariable]]
        environment = _environment.asMap

        importId <- Either.fromOption(
          environment.get(ImportId),
          DecodingFailure(s"missing environment variable: $ImportId", c.history)
        )

        manifestUri <- Either.fromOption(
          environment.get("PARAMS_FILE"),
          DecodingFailure("missing environment variable: PARAMS_FILE", c.history)
        )

        uuid <- Try { UUID.fromString(importId) }.toEither
          .leftMap(_ => DecodingFailure("invalid importId: not a UUID", c.history))

        sentAt <- detail.downField("updatedAt").as[String]

        jobState = exitCode match {
          case Some(code) if code == 0 => JobState.Succeeded
          case _ => JobState.Failed
        }

      } yield
        CloudwatchMessage(
          JobId(uuid),
          TaskId(taskArn, clusterArn),
          jobState,
          ManifestUri(URLDecoder.decode(manifestUri, UTF_8.toString)),
          OffsetDateTime.parse(sentAt)
        )
    }

  val runningTaskEventDecoder: Decoder[CloudwatchMessage] =
    (c: HCursor) => {
      val detail = c.downField("detail")

      for {
        taskArn <- detail.get[String]("taskArn")
        clusterArn <- detail.get[String]("clusterArn")

        lastStatus <- detail
          .downField("containers")
          .downArray
          .downField("lastStatus")
          .as[String]

        _environment <- detail
          .downField("overrides")
          .downField("containerOverrides")
          .downArray
          .downField("environment")
          .as[List[AWSEnvironmentVariable]]
        environment = _environment.asMap

        importId <- Either.fromOption(
          environment.get(ImportId),
          DecodingFailure(s"missing environment variable: $ImportId", c.history)
        )

        manifestUri <- Either.fromOption(
          environment.get("PARAMS_FILE"),
          DecodingFailure("missing environment variable: PARAMS_FILE", c.history)
        )

        uuid <- Try { UUID.fromString(importId) }.toEither
          .leftMap(_ => DecodingFailure("invalid importId: not a UUID", c.history))

        sentAt <- detail.downField("updatedAt").as[String]

        jobState <- lastStatus match {
          case "PENDING" => Right(JobState.Pending)
          case "RUNNING" => Right(JobState.Running)
          case status => Left(DecodingFailure(s"unexpected status $status", c.history))
        }

      } yield
        CloudwatchMessage(
          JobId(uuid),
          TaskId(taskArn, clusterArn),
          jobState,
          ManifestUri(URLDecoder.decode(manifestUri, UTF_8.toString)),
          OffsetDateTime.parse(sentAt)
        )
    }
}
