// Copyright (c) [2018] - [2021] Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.jobscheduling.clients
import cats.data.EitherT
import cats.implicits._
import com.pennsieve.jobscheduling.ETLLogContext
import com.pennsieve.jobscheduling.commons.JobState
import com.pennsieve.jobscheduling.commons.JobState.{ NotProcessing, Succeeded }
import com.pennsieve.jobscheduling.clients.SQSClient.{ MessageBody, SendMessage }
import com.pennsieve.jobscheduling.db.JobStateHelpers.{ isSuccess, shouldSendNotification }
import com.pennsieve.jobscheduling.model.EventualResult.EventualResultT
import com.pennsieve.models.{
  ETLAppendWorkflow,
  ETLExportWorkflow,
  ETLWorkflow,
  FileType,
  JobId,
  PackageState,
  PackageType,
  Payload,
  PayloadType,
  Upload
}
import com.pennsieve.notifications.{
  ETLExportNotification,
  ETLNotification,
  MessageType,
  NotificationMessage,
  UploadNotification
}
import com.pennsieve.service.utilities.ContextLogger
import io.circe.Encoder
import io.circe.generic.semiauto.deriveEncoder
import io.circe.syntax._

import scala.concurrent.{ ExecutionContext, Future }

object Notifications {

  implicit val etlNotificationMessageEncoder: Encoder[NotificationMessage] =
    deriveEncoder[NotificationMessage]

  def sendNotification(
    sender: String,
    jobId: JobId,
    organizationId: Int,
    jobState: JobState,
    payload: Payload,
    sendMessage: SendMessage
  )(implicit
    log: ContextLogger,
    ec: ExecutionContext
  ): EventualResultT[Unit] = {
    implicit val logContext =
      ETLLogContext(importId = Some(jobId), organizationId = Some(organizationId))

    val success = isSuccess(jobState)

    def toImportNotification(
      payloadType: PayloadType,
      fileType: FileType,
      packageType: PackageType,
      uploadedFiles: List[String]
    ): NotificationMessage =
      ETLNotification(
        users = List(payload.userId),
        messageType = MessageType.JobDone,
        success = success,
        jobType = payloadType,
        importId = jobId.value.toString,
        organizationId = organizationId,
        packageId = payload.packageId,
        datasetId = payload.datasetId,
        uploadedFiles = uploadedFiles,
        fileType = fileType,
        packageType = packageType,
        message = s"$payloadType job complete"
      )

    def toExportNotification(
      payloadType: PayloadType,
      packageType: PackageType,
      fileType: FileType,
      sourcePackageId: Int,
      sourcePackageType: PackageType
    ): NotificationMessage =
      ETLExportNotification(
        users = List(payload.userId),
        messageType = MessageType.JobDone,
        success = success,
        jobType = payloadType,
        importId = jobId.value.toString,
        organizationId = organizationId,
        packageId = payload.packageId,
        datasetId = payload.datasetId,
        fileType = fileType,
        packageType = packageType,
        sourcePackageId = sourcePackageId,
        sourcePackageType = sourcePackageType,
        message = s"$payloadType job complete"
      )

    def notifyUser(message: NotificationMessage) = {
      log.context.info(s"""$sender sending $message""")

      EitherT(sendMessage(MessageBody(message.asJson.noSpaces)))
        .map(_ => ())
    }

    val shouldSend = shouldSendNotification(jobState)

    payload match {
      case append: ETLAppendWorkflow if shouldSend =>
        notifyUser(
          toImportNotification(
            PayloadType.Append,
            append.fileType,
            append.packageType,
            append.files
          )
        )

      case export: ETLExportWorkflow if shouldSend =>
        notifyUser(
          toExportNotification(
            PayloadType.Export,
            export.packageType,
            export.fileType,
            export.sourcePackageId,
            export.sourcePackageType
          )
        )

      case workflow: ETLWorkflow if shouldSend =>
        notifyUser(
          toImportNotification(
            PayloadType.Workflow,
            workflow.fileType,
            workflow.packageType,
            workflow.files
          )
        )

      case upload: Upload if shouldSend =>
        notifyUser(
          UploadNotification(
            users = List(upload.userId),
            success = success,
            datasetId = upload.datasetId,
            packageId = upload.packageId,
            organizationId = organizationId,
            uploadedFiles = upload.files
          )
        )

      case _ =>
        log.context.info(s"$sender not sending notification for payload: $payload")
        EitherT.pure[Future, Throwable](Unit)
    }
  }
}
