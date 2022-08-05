// Copyright (c) [2018] - [2022] Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.jobscheduling.pusher

import java.util.UUID

import akka.actor.{ ActorSystem, Scheduler }
import akka.stream.scaladsl.{ Sink, Source }
import akka.testkit.TestKitBase
import cats.implicits._
import com.amazonaws.AmazonServiceException
import com.amazonaws.services.ecs.model._
import com.pennsieve.jobscheduling.JobSchedulingPorts.GetPayload
import com.pennsieve.jobscheduling.TestConfig.{ staticEcsConfig, staticS3Config }
import com.pennsieve.jobscheduling.TestTask._
import com.pennsieve.jobscheduling.commons.JobState
import com.pennsieve.jobscheduling.db.{ Job, PayloadEntry }
import com.pennsieve.jobscheduling.errors.NoPayloadForJob
import com.pennsieve.jobscheduling.model.ETLEvent
import com.pennsieve.jobscheduling.model.JobConverters.{ ErrorWithJobConverter, RichJob }
import com.pennsieve.jobscheduling.pusher.JobPusherFakes._
import com.pennsieve.jobscheduling.pusher.JobPusherPorts._
import com.pennsieve.jobscheduling.{ ECSConfig, FakeFargate }
import com.pennsieve.models.PayloadType._
import com.pennsieve.models.{
  Channel,
  ETLAppendWorkflow,
  ETLExportWorkflow,
  FileType,
  JobId,
  Manifest,
  PackageType
}
import com.pennsieve.service.utilities.ContextLogger
import com.pennsieve.test.AwaitableImplicits
import io.circe.parser.decode
import org.scalatest._

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future }
import scala.io.{ Source => ScalaSource }

class JobPusherSpec
    extends WordSpecLike
    with TestKitBase
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with AwaitableImplicits {

  implicit lazy val system = ActorSystem("JobPusherSpec")

  implicit val log: ContextLogger = new ContextLogger
  implicit val executionContext: ExecutionContext = system.dispatcher
  implicit val scheduler: Scheduler = system.scheduler
  val etlBucket: String = staticS3Config.etlBucket

  "A Pusher" should {
    "push a task for a job" in {
      val pusher =
        new JobPusher(
          pusherPorts(),
          defaultPusherConfig,
          staticS3Config.etlBucket,
          defaultEcsConfig
        )

      val event = push(pusher)(job)
      event shouldBe someSuccessfulEvent(event)
    }

    "retry a task if it is over it's task limit" in {
      val fakeFargate = new FakeFargate()

      val pusher =
        new JobPusher(
          pusherPorts(fakeFargate.runTask()),
          defaultPusherConfig,
          staticS3Config.etlBucket,
          defaultEcsConfig
        )

      val event = push(pusher)(job)
      event shouldBe someSuccessfulEvent(event)
    }

    "fail a task if it repeatedly fails" in {
      val amazonServiceException = new AmazonServiceException("Failed")
      val expectedEtlEvent =
        (JobRejected, job).toTaskCreationFailedEvent(etlBucket)

      val failingRunTaskAsync: RunTask =
        (_: RunTaskRequest) => Future.successful(Left(amazonServiceException))

      val pusher =
        new JobPusher(
          pusherPorts(failingRunTaskAsync),
          defaultPusherConfig,
          staticS3Config.etlBucket,
          ecsConfigOneRetry
        )

      val event = push(pusher)(job)
      event shouldBe Some(expectedEtlEvent.copy(sentAt = event.get.sentAt))
    }

    "fail a task if the response contains failed tasks" in {
      val expectedEtlEvent =
        (JobRejected, job).toTaskCreationFailedEvent(etlBucket)

      val jobFailingRunTaskAsync =
        (_: RunTaskRequest) =>
          Future.successful {
            val taskResult = new RunTaskResult()
            taskResult.setFailures(List(new Failure()).asJavaCollection)
            Right(taskResult)
          }

      val pusher =
        new JobPusher(
          ports = pusherPorts(jobFailingRunTaskAsync),
          pusherConfig = defaultPusherConfig,
          staticS3Config.etlBucket,
          ecsConfigOneRetry
        )

      val event = push(pusher)(job)
      event shouldBe Some(expectedEtlEvent.copy(sentAt = event.get.sentAt))
    }

    "fail if manifest cannot be uploaded" in {
      val manifestUploadFailure = ManifestUploadFailure(new AmazonServiceException("amazon failed"))
      val expectedEtlEvent =
        (manifestUploadFailure, job).toTaskCreationFailedEvent(etlBucket)

      val failingManifestPut: PutManifest =
        (_, _) => Future.successful(Left(manifestUploadFailure))

      val pusher =
        new JobPusher(
          JobPusherPorts(successfulRunTask, failingManifestPut, stubManifestGet),
          pusherConfig = defaultPusherConfig,
          staticS3Config.etlBucket,
          defaultEcsConfig
        )

      val event = push(pusher)(job)
      event shouldBe Some(expectedEtlEvent.copy(sentAt = event.get.sentAt))
    }

    "fail if payload cannot be found" in {
      val expectedEtlEvent =
        (NoPayloadForJob, job).toTaskCreationFailedEvent(etlBucket)
      val failingGetPayload: GetPayload =
        _ => Future.successful(None)

      val pusher =
        new JobPusher(
          pusherPorts(getPayload = failingGetPayload),
          defaultPusherConfig,
          staticS3Config.etlBucket,
          defaultEcsConfig
        )

      val event = push(pusher)(job)
      event shouldBe Some(expectedEtlEvent.copy(sentAt = event.get.sentAt))
    }

    "fail if request for payload fails" in {
      val expectedException = new Exception("client failure")
      val expectedEtlEvent =
        (GetPayloadException(expectedException), job)
          .toTaskCreationFailedEvent(etlBucket)

      val failingGetPayload: GetPayload =
        _ => Future.failed(expectedException)

      val pusher =
        new JobPusher(
          pusherPorts(getPayload = failingGetPayload),
          defaultPusherConfig,
          staticS3Config.etlBucket,
          defaultEcsConfig
        )

      val result = push(pusher)(job)
      result shouldBe Some(expectedEtlEvent.copy(sentAt = result.get.sentAt))
    }

    "accept an append payload type" in {
      val appendPayload: ETLAppendWorkflow =
        ETLAppendWorkflow(
          packageId = 1,
          datasetId = 1,
          userId = 1,
          fileType = FileType.AFNI,
          packageType = PackageType.CSV,
          files = List.empty[String],
          assetDirectory = "storage",
          encryptionKey = "key",
          channels = List.empty[Channel]
        )

      val differentTypeGetPayload: GetPayload =
        _ => Future.successful(Some(PayloadEntry(appendPayload)))

      var sentManifest = Option.empty[Manifest]

      val saveManifestPutManifest: PutManifest =
        (manifestBytes, _) => {
          sentManifest = decode[Manifest] {
            ScalaSource
              .fromBytes(manifestBytes.bytes)
              .getLines()
              .mkString("\n")
          }.toOption

          Future.successful(().asRight)
        }

      val pusher =
        new JobPusher(
          pusherPorts(getPayload = differentTypeGetPayload, putManifest = saveManifestPutManifest),
          defaultPusherConfig,
          staticS3Config.etlBucket,
          defaultEcsConfig
        )

      val event = push(pusher)(job)

      sentManifest.map(_.`type`) shouldBe Append.some

      event shouldBe someSuccessfulEvent(event)
    }

    "accept an export payload type" in {
      val exportPayload: ETLExportWorkflow =
        ETLExportWorkflow(
          packageId = 1,
          datasetId = 1,
          userId = 1,
          encryptionKey = "key",
          packageType = PackageType.TimeSeries,
          fileType = FileType.NeuroDataWithoutBorders,
          sourcePackageId = 2,
          sourcePackageType = PackageType.HDF5
        )

      val differentTypeGetPayload: GetPayload =
        _ => Future.successful(Some(PayloadEntry(exportPayload)))

      var sentManifest = Option.empty[Manifest]

      val saveManifestPutManifest: PutManifest =
        (manifestBytes, _) => {
          sentManifest = decode[Manifest] {
            ScalaSource
              .fromBytes(manifestBytes.bytes)
              .getLines()
              .mkString("\n")
          }.toOption

          Future.successful(().asRight)
        }

      val pusher =
        new JobPusher(
          pusherPorts(getPayload = differentTypeGetPayload, putManifest = saveManifestPutManifest),
          defaultPusherConfig,
          staticS3Config.etlBucket,
          defaultEcsConfig
        )

      val event = push(pusher)(job)

      sentManifest.map(_.`type`) shouldBe Export.some

      event shouldBe someSuccessfulEvent(event)
    }

    "accept a etl workflow payload" in {
      var sentManifest = Option.empty[Manifest]

      val saveManifestPutManifest: PutManifest =
        (manifestBytes, _) => {
          sentManifest = decode[Manifest] {
            ScalaSource
              .fromBytes(manifestBytes.bytes)
              .getLines()
              .mkString("\n")
          }.toOption

          Future.successful(().asRight)
        }

      val pusher =
        new JobPusher(
          pusherPorts(putManifest = saveManifestPutManifest),
          defaultPusherConfig,
          staticS3Config.etlBucket,
          defaultEcsConfig
        )

      val event = push(pusher)(job)

      sentManifest.map(_.`type`) shouldBe Workflow.some

      event shouldBe someSuccessfulEvent(event)
    }
  }

  private def someSuccessfulEvent(event: Option[ETLEvent]) =
    Some(job.toSuccessfulEvent(etlBucket, taskId, JobState.Submitted, sentAt = event.get.sentAt))

  def push(pusher: JobPusher)(job: Job): Option[ETLEvent] =
    Source.single(job).via(pusher.flow).runWith(Sink.headOption).awaitFinite(10 seconds)

  private val ecsConfigOneRetry: ECSConfig = staticEcsConfig
    .copy(task = staticEcsConfig.task.copy(maxAttempts = 1))

  val job = Job(
    id = JobId(UUID.randomUUID()),
    payloadId = 1,
    organizationId = 1,
    userId = Some(1),
    state = JobState.Uploading
  )
}
