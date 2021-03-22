// Copyright (c) [2018] - [2021] Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.jobscheduling.model

import cats.implicits._

import com.amazonaws.services.ecs.model._
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

import scala.collection.JavaConverters.asJavaCollectionConverter

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

      val vpcConfiguration = new AwsVpcConfiguration()
        .withSecurityGroups(ecsConfig.securityGroups.asJavaCollection)
        .withSubnets(ecsConfig.subnetIds.asJavaCollection)
        .withAssignPublicIp("DISABLED")

      val networkConfiguration = new NetworkConfiguration()
        .withAwsvpcConfiguration(vpcConfiguration)

      val nextflowEnvironmentVariables = List(
        new KeyValuePair().withName(ImportId).withValue(jobId.toString),
        new KeyValuePair().withName(PayloadId).withValue(job.payloadId.toString),
        new KeyValuePair()
          .withName("WORKING_DIR")
          .withValue(workingDirectory),
        new KeyValuePair().withName("PARAMS_FILE").withValue(manifestUri),
        new KeyValuePair().withName("MANIFEST_KEY").withValue(manifestPath),
        new KeyValuePair()
          .withName("ENVIRONMENT")
          .withValue(pusherConfig.environment),
        new KeyValuePair()
          .withName("NEXTFLOW_IAM_ACCESS_KEY_ID")
          .withValue(ecsConfig.nextflow.accessKeyId),
        new KeyValuePair()
          .withName("NEXTFLOW_IAM_ACCESS_KEY_SECRET")
          .withValue(ecsConfig.nextflow.accessKeySecret)
      ).asJavaCollection

      val containerOverride = new ContainerOverride()
        .withName("nextflow")
        .withCpu(ecsConfig.task.containerCPU)
        .withMemory(ecsConfig.task.containerMemory)
        .withEnvironment(nextflowEnvironmentVariables)

      val taskOverride = new TaskOverride()
        .withTaskRoleArn(ecsConfig.task.iamRole)
        .withExecutionRoleArn(ecsConfig.task.iamRole)
        .withContainerOverrides(containerOverride)

      new RunTaskRequest()
        .withCluster(ecsConfig.cluster)
        .withTaskDefinition(ecsConfig.task.taskDefinition)
        .withOverrides(taskOverride)
        .withCount(1)
        .withStartedBy(JobSchedulingService)
        .withLaunchType("FARGATE")
        .withNetworkConfiguration(networkConfiguration)
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
