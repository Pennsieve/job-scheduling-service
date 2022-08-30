// Copyright (c) [2018] - [2022] Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.jobscheduling.model

import cats.implicits._

import software.amazon.awssdk.services.ecs.model._
import com.pennsieve.jobscheduling.clients.ManifestS3
import com.pennsieve.jobscheduling.commons.JobState
import com.pennsieve.jobscheduling.db.{ Job, PayloadEntry, TaskId }
import com.pennsieve.jobscheduling.pusher.JobPusher.ErrorWithJob
import com.pennsieve.jobscheduling.clients.SQSClient.ReceiptHandle
import com.pennsieve.jobscheduling.{ ECSConfig, PusherConfig }
import com.pennsieve.models.PayloadType
import com.pennsieve.models.{
  ETLAppendWorkflow,
  ETLExportWorkflow,
  ETLWorkflow,
  JobId,
  Manifest,
  PackageState,
  Payload,
  PayloadType,
  Upload,
  Workflow
}
import java.time.OffsetDateTime
import java.time.ZoneOffset.UTC

import com.pennsieve.jobscheduling.errors.{ NoPayloadForJob, UnsupportedPayload }
import com.pennsieve.jobscheduling.server.generated.definitions
import io.circe.syntax.EncoderOps

import scala.jdk.CollectionConverters._

object JobConverters {
  val ImportId = "IMPORT_ID"
  val PayloadId = "PAYLOAD_ID"
  val JobSchedulingService = "job-scheduling-service"

  def getManifestPath(jobId: JobId): String =
    s"jobs/$jobId/manifest.json"

  implicit def manifestToManifestS3(manifest: Manifest): ManifestS3 =
    ManifestS3(uri = getManifestPath(manifest.importId), bytes = manifest.asJson.toString.getBytes)

  implicit class RichJob(job: Job) {
    def toRunTaskRequest(
      bucket: String,
      pusherConfig: PusherConfig,
      ecsConfig: ECSConfig
    ): RunTaskRequest = {
      val jobId = job.id
      val jobPath = s"jobs/$jobId"
      val manifestPath = getManifestPath(jobId)
      val manifestUri = ManifestUri(bucket, jobId).value
      val workingDirectory = s"s3://$bucket/$jobPath/scratch"

      val vpcConfiguration: AwsVpcConfiguration = AwsVpcConfiguration
        .builder()
        .securityGroups(ecsConfig.securityGroups.asJava)
        .subnets(ecsConfig.subnetIds.asJava)
        .assignPublicIp("DISABLED")
        .build()

      val networkConfiguration: NetworkConfiguration = NetworkConfiguration
        .builder()
        .awsvpcConfiguration(vpcConfiguration)
        .build()

      val nextflowEnvironmentVariables = List(
        KeyValuePair.builder().name(ImportId).value(jobId.toString).build(),
        KeyValuePair.builder().name(PayloadId).value(job.payloadId.toString).build(),
        KeyValuePair
          .builder()
          .name("WORKING_DIR")
          .value(workingDirectory)
          .build(),
        KeyValuePair.builder().name("PARAMS_FILE").value(manifestUri).build(),
        KeyValuePair.builder().name("MANIFEST_KEY").value(manifestPath).build(),
        KeyValuePair
          .builder()
          .name("ENVIRONMENT")
          .value(pusherConfig.environment)
          .build(),
        KeyValuePair
          .builder()
          .name("NEXTFLOW_IAM_ACCESS_KEY_ID")
          .value(ecsConfig.nextflow.accessKeyId)
          .build(),
        KeyValuePair
          .builder()
          .name("NEXTFLOW_IAM_ACCESS_KEY_SECRET")
          .value(ecsConfig.nextflow.accessKeySecret)
          .build()
      ).asJava

      val containerOverride: ContainerOverride = ContainerOverride
        .builder()
        .name("nextflow")
        .cpu(ecsConfig.task.containerCPU)
        .memory(ecsConfig.task.containerMemory)
        .environment(nextflowEnvironmentVariables)
        .build()

      val taskOverride: TaskOverride = TaskOverride
        .builder()
        .taskRoleArn(ecsConfig.task.iamRole)
        .executionRoleArn(ecsConfig.task.iamRole)
        .containerOverrides(containerOverride)
        .build()

      RunTaskRequest
        .builder()
        .cluster(ecsConfig.cluster)
        .taskDefinition(ecsConfig.task.taskDefinition)
        .overrides(taskOverride)
        .count(1)
        .startedBy(JobSchedulingService)
        .launchType("FARGATE")
        .networkConfiguration(networkConfiguration)
        .build()
    }

    import io.scalaland.chimney.dsl.TransformerOps
    def toSwaggerJob: definitions.Job =
      job
        .into[definitions.Job]
        .transform

    def toSuccessfulEvent(
      bucket: String,
      taskId: TaskId,
      jobState: JobState,
      receiptHandle: Option[ReceiptHandle] = None,
      sentAt: OffsetDateTime = OffsetDateTime.now(UTC)
    ): ETLEvent =
      SuccessfulEvent(
        job.id,
        taskId,
        jobState,
        Some(job.payloadId),
        ManifestUri(bucket, job.id),
        job.organizationId,
        job.userId,
        receiptHandle,
        sentAt
      )
  }

  implicit class ErrorWithJobConverter(errorWithJob: ErrorWithJob) {
    val (error, job) = errorWithJob
    def toTaskCreationFailedEvent(bucket: String): TaskCreationFailedEvent =
      TaskCreationFailedEvent(
        job.id,
        ManifestUri(bucket, job.id),
        Some(job.payloadId),
        error,
        job.organizationId,
        job.userId
      )
  }

  def payloadToManifest(job: Job)(payload: Option[Payload]): Either[Exception, Manifest] = {
    def getPayloadType: Payload => Either[Exception, PayloadType] = {
      case upload: Upload =>
        Right(PayloadType.Upload)
      case workflow: ETLWorkflow =>
        Right(PayloadType.Upload)
      case workflow: ETLExportWorkflow =>
        Right(PayloadType.Export)
      case workflow: ETLAppendWorkflow =>
        Right(PayloadType.Append)
      case unsupportedPayload =>
        Left(UnsupportedPayload(unsupportedPayload))
    }

    for {
      somePayload <- Either.fromOption(payload, NoPayloadForJob)
      payloadType <- getPayloadType(somePayload)
    } yield
      Manifest(
        `type` = payloadType,
        importId = job.id,
        organizationId = job.organizationId,
        content = somePayload
      )
  }

}
