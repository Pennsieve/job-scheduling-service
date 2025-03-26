// Copyright (c) [2018] - [2025] Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.jobscheduling.model
import com.pennsieve.jobscheduling.commons.JobState
import com.pennsieve.jobscheduling.db.TaskId
import com.pennsieve.jobscheduling.model.JobConverters.getManifestPath
import com.pennsieve.jobscheduling.clients.SQSClient.ReceiptHandle
import com.pennsieve.models.{ JobId, PackageState }
import io.circe.{ Encoder, Json }
import io.circe.syntax._
import io.circe.generic.semiauto.deriveEncoder
import java.time.OffsetDateTime
import java.time.ZoneOffset.UTC

import com.pennsieve.jobscheduling.ETLLogContext

sealed trait ETLEvent {
  val importId: JobId
  val organizationId: Int
  val jobState: JobState
  val manifestUri: ManifestUri
  val userId: Option[Int]
  val payloadId: Option[Int]
  val receiptHandle: Option[ReceiptHandle]
  val sentAt: OffsetDateTime

  val logContext: ETLLogContext =
    ETLLogContext(
      importId = Some(this.importId),
      organizationId = Some(this.organizationId),
      userId = this.userId
    )
}

object ETLEvent {

  import OffsetDateTimeEncoder._

  implicit val throwableEncoder: Encoder[Throwable] =
    Encoder.forProduct1("throwable")(_.toString)

  implicit val successfulEncoder: Encoder[SuccessfulEvent] = deriveEncoder[SuccessfulEvent]

  implicit val lostTaskEncoder: Encoder[LostTaskEvent] = deriveEncoder[LostTaskEvent]

  implicit val failedLostTaskEncoder: Encoder[FailedLostTaskEvent] =
    deriveEncoder[FailedLostTaskEvent]

  implicit val taskCreationFailureEvent: Encoder[TaskCreationFailedEvent] =
    deriveEncoder[TaskCreationFailedEvent]

  implicit val packageLostEvent: Encoder[PackageLostEvent] =
    deriveEncoder[PackageLostEvent]

  implicit val etlEventEncoder: Encoder[ETLEvent] = {
    case successful: SuccessfulEvent => successful.asJson
    case lostTask: LostTaskEvent => lostTask.asJson
    case taskCreationFailed: TaskCreationFailedEvent => taskCreationFailed.asJson
    case failedLostTask: FailedLostTaskEvent => failedLostTask.asJson
    case packageLostEvent: PackageLostEvent => packageLostEvent.asJson
  }
}

final case class SuccessfulEvent(
  importId: JobId,
  taskId: TaskId,
  jobState: JobState,
  payloadId: Option[Int],
  manifestUri: ManifestUri,
  organizationId: Int,
  userId: Option[Int] = None,
  receiptHandle: Option[ReceiptHandle] = None,
  sentAt: OffsetDateTime = OffsetDateTime.now(UTC)
) extends ETLEvent

final case class PackageLostEvent(
  importId: JobId,
  payloadId: Option[Int],
  manifestUri: ManifestUri,
  organizationId: Int,
  userId: Option[Int] = None,
  receiptHandle: Option[ReceiptHandle] = None,
  sentAt: OffsetDateTime = OffsetDateTime.now(UTC)
) extends ETLEvent {
  override val jobState: JobState = JobState.Cancelled
}

final case class LostTaskEvent(
  importId: JobId,
  jobState: JobState,
  payloadId: Option[Int],
  manifestUri: ManifestUri,
  organizationId: Int,
  userId: Option[Int] = None,
  receiptHandle: Option[ReceiptHandle] = None,
  sentAt: OffsetDateTime = OffsetDateTime.now(UTC)
) extends ETLEvent

final case class TaskCreationFailedEvent(
  importId: JobId,
  manifestUri: ManifestUri,
  payloadId: Option[Int],
  cause: Throwable,
  organizationId: Int,
  userId: Option[Int] = None,
  sentAt: OffsetDateTime = OffsetDateTime.now(UTC)
) extends ETLEvent {
  override val jobState: JobState = JobState.Failed
  override val receiptHandle: Option[ReceiptHandle] = None
}

final case class FailedLostTaskEvent(
  importId: JobId,
  manifestUri: ManifestUri,
  organizationId: Int,
  userId: Option[Int] = None,
  payloadId: Option[Int],
  sentAt: OffsetDateTime = OffsetDateTime.now(UTC)
) extends ETLEvent {
  override val jobState: JobState = JobState.Failed
  override val receiptHandle: Option[ReceiptHandle] = None
}

final case class ManifestUri(value: String) extends AnyVal {
  override def toString: String = value
}

object ManifestUri {

  lazy implicit val encoder: Encoder[ManifestUri] = deriveEncoder[ManifestUri]

  def apply(bucket: String, jobId: JobId): ManifestUri =
    ManifestUri(s"s3://$bucket/${getManifestPath(jobId)}")
}
