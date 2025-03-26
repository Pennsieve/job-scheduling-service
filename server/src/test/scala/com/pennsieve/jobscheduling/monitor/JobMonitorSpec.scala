// Copyright (c) [2018] - [2025] Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.jobscheduling.monitor

import java.time.OffsetDateTime
import java.time.ZoneOffset.UTC
import java.util.UUID
import akka.actor.{ ActorSystem, Scheduler }
import akka.http.scaladsl.model.StatusCode
import akka.stream.scaladsl.{ Sink, Source }
import akka.testkit.TestKit
import cats.data.EitherT
import software.amazon.awssdk.services.sqs.model.{
  DeleteMessageResponse,
  Message,
  SendMessageResponse
}
import com.pennsieve.jobscheduling.JobSchedulingPorts.{
  createGetJob,
  createUpdateJob,
  GetJob,
  GetManifest,
  GetPayload,
  NotifyJobSource,
  UpdateJob
}

import scala.concurrent.duration.FiniteDuration
import com.pennsieve.jobscheduling.{
  JobMonitorConfig,
  JobSchedulingServiceSpecHarness,
  RetryConfig
}
import com.pennsieve.jobscheduling.Fakes._
import com.pennsieve.jobscheduling.TestPayload._
import com.pennsieve.jobscheduling.TestTask.taskId
import com.pennsieve.jobscheduling.clients.SQSClient.{
  MessageBody,
  ReceiptHandle,
  SendAck,
  SendMessage
}
import com.pennsieve.jobscheduling.db.JobsMapper.createWithPayload
import com.pennsieve.jobscheduling.db.profile.api._
import com.pennsieve.jobscheduling.db.{
  Job,
  JobsMapper,
  OrganizationQuotaMapper,
  PayloadEntry,
  TaskId
}
import com.pennsieve.jobscheduling.model.EventualResult.EitherContext
import com.pennsieve.jobscheduling.model.{ ETLEvent, ManifestUri }
import com.pennsieve.models.{ PayloadType, _ }
import com.pennsieve.notifications.{
  ETLExportNotification,
  ETLNotification,
  MessageType,
  NotificationMessage,
  UploadNotification
}
import io.circe.Decoder
import io.circe.generic.semiauto._
import io.circe.syntax.EncoderOps
import cats.implicits._
import com.pennsieve.auth.middleware.{ OrganizationId, UserId }
import com.pennsieve.jobscheduling.clients.PennsieveApiClient
import com.pennsieve.jobscheduling.commons.JobState
import com.pennsieve.jobscheduling.commons.JobState._
import com.pennsieve.jobscheduling.db.JobStateHelpers._
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ ExecutionContext, Future, Promise }
import scala.concurrent.duration.DurationLong

class JobMonitorSpec(system: ActorSystem)
    extends TestKit(system)
    with JobSchedulingServiceSpecHarness
    with AnyWordSpecLike
    with Matchers
    with BeforeAndAfterEach {

  override val StopContainersTimeout = 15.seconds

  def this() = this(ActorSystem("JobMonitorSpec"))

  implicit val actorSystem: ActorSystem = system
  implicit val executionContext: ExecutionContext = actorSystem.dispatcher
  implicit val scheduler: Scheduler = actorSystem.scheduler
  implicit val etlNotificationDecoder: Decoder[ETLNotification] =
    deriveDecoder[ETLNotification]

  override def beforeEach(): Unit = {
    ports.db.run(JobsMapper.delete).awaitFinite()
    ports.db.run(OrganizationQuotaMapper.delete).awaitFinite()
    ports.db.run(OrganizationQuotaMapper.create(organizationId, 10)).awaitFinite()
  }

  "JobMonitor" should {
    "update the package status" in {
      val insertedJob = insertJobAndPayload()

      List(failedEcsEvent(insertedJob.id)).foreach {
        case (eventMessage, state) =>
          val states = ArrayBuffer[PackageState]()

          runStream(eventMessage, fakePennsieveApiClient())

          states.headOption.contains(PackageState.READY) shouldBe isSuccess(state)
      }
    }

    "package state should not be updated for append" in {
      val insertedJob = insertJobAndPayload(appendPayload)
      List(failedEcsEvent(insertedJob.id)).foreach {
        case (eventMessage, state) =>
          val states = ArrayBuffer[PackageState]()
          val savingStatePackage: SetPackageState =
            (_, _, _, state) => {
              states.append(state)
              EitherT(Future.successful("".asRight[(StatusCode, Throwable)]))
            }
          runStream(eventMessage, fakePennsieveApiClient(savingStatePackage))
          states.headOption shouldBe None
      }
    }

    "update database job state one by one in order" in {
      val insertedJobId = insertJobAndPayload(appendPayload).id

      val eventJsonWithExpectedJobState = List(
        pendingEcsEvent(insertedJobId, FiveHoursAhead.minusSeconds(3L).toString),
        runningEcsEvent(insertedJobId, FiveHoursAhead.minusSeconds(2L).toString),
        succeededEcsEvent(insertedJobId, FiveHoursAhead.minusSeconds(1L).toString),
        failedEcsEvent(insertedJobId, FiveHoursAhead.toString)
      )

      eventJsonWithExpectedJobState.foreach {
        case (eventMessage, state) =>
          runStream(eventMessage) should not be None

          val actualJob = ports.db.run(JobsMapper.get(insertedJobId)).awaitFinite().get

          actualJob.id shouldBe insertedJobId
          actualJob.state shouldBe state
      }

      val actualState = ports.db
        .run(JobsMapper.get(insertedJobId))
        .awaitFinite()
        .get
        .state

      actualState shouldBe JobState.Failed
    }

    "update database job state if messages come out of order" in {
      val insertedJob = insertJobAndPayload(appendPayload)

      val oneHourEarlier = FiveHoursAhead.minusHours(1L).toString
      val oldSuccessfulEvent = succeededEcsEvent(insertedJob.id, time = oneHourEarlier)

      val failureFollowedByOldSuccess =
        List(failedEcsEvent(insertedJob.id, FiveHoursAhead.toString), oldSuccessfulEvent)

      failureFollowedByOldSuccess.foreach {
        case (eventMessage, _) =>
          runStream(eventMessage) should not be None

          val actualJob = ports.db.run(JobsMapper.get(insertedJob.id)).awaitFinite().get

          // the first message indicates that this job has failed,
          // other messages come in after saying 'success', but they
          // should not be applied to this job because their sentAt is
          // less than the first message
          actualJob.id shouldBe insertedJob.id
          actualJob.state shouldBe JobState.Failed
      }

      ports.db
        .run(JobsMapper.get(insertedJob.id))
        .awaitFinite()
        .get
        .state shouldBe JobState.Failed
    }

    "not update package state or send a notification if database is failing" in {
      testMessages(defaultJobId).foreach {
        case (eventMessage, _) =>
          var sentNotification: Option[String] = None

          runStream(
            eventMessage,
            fakePennsieveApiClient(notImplementedSetPackageState),
            updateJob = (_, _, _, _, _) => Future.failed[EitherContext[Unit]](new Exception()),
            getJob = _ => Future.failed[Option[Job]](new Exception("Test database is down")),
            getPayload = _ => Future.failed[Option[PayloadEntry]](new Exception())
          )

          sentNotification shouldBe None
      }
    }

    "send an ack message for a completed cloudwatch message" in {
      val insertedJob = insertJobAndPayload()

      testMessages(insertedJob.id).foreach {
        case (eventMessage, _) =>
          val (acknowledged, sendAck) = createSendAck

          runStream(eventMessage, sendAck = sendAck)

          acknowledged.awaitFinite() shouldBe receiptHandle
      }
    }

    "send an ack message when it is unable to decode a message" in {
      val event = succeededEcsEvent(defaultJobId)
      val badEvent = s"""{"wrapped": $event}"""

      val (acknowledged, sendAck) = createSendAck

      runStream(badEvent, sendAck = sendAck)

      acknowledged.awaitFinite() shouldBe receiptHandle
    }

    "send an ack message when job is not available in database" in {
      testMessages(defaultJobId).foreach {
        case (eventMessage, _) =>
          val (acknowledged, sendAck) = createSendAck

          runStream(eventMessage, sendAck = sendAck)

          acknowledged.awaitFinite() shouldBe receiptHandle
      }
    }

    "re-run the stream on failure" in {
      val insertedJob = insertJobAndPayload()
      testMessages(insertedJob.id).foreach {
        case (eventMessage, desiredState) =>
          val (acknowledged, sendAck) = createSendAck

          // this will fail on the first 2 attempts
          runStream(eventMessage, updateJob = failingUpdateJob(ports.db, 2), sendAck = sendAck)

          // should have updated the job in the database
          ports.db
            .run(JobsMapper.get(insertedJob.id))
            .awaitFinite()
            .get
            .state shouldBe desiredState

          // should have acked the message
          acknowledged.awaitFinite() shouldBe receiptHandle
      }
    }

    "re-run the stream on failure, fail if maxRetries is exceeded" in {
      val insertedJob = insertJobAndPayload()
      testMessages(insertedJob.id).foreach {
        case (eventMessage, desiredState) =>
          // this will fail on the first 4 attempts, exceeding maxRetries
          val exception = the[Exception] thrownBy runStream(
            eventMessage,
            updateJob = failingUpdateJob(ports.db, 5),
            timeout = 10 seconds
          )

          exception.getMessage shouldBe "This is just a test."
      }
    }

    "re-run the stream on failure, reset retry count after limit is reached" in {
      val insertedJob = insertJobAndPayload()
      testMessages(insertedJob.id).foreach {
        case (eventMessage, desiredState) =>
          val (acknowledged, sendAck) = createSendAck

          // this will fail on the first 4 attempts
          // this function should keep retrying regardless
          runStream(
            eventMessage,
            updateJob = failingUpdateJob(ports.db, 5),
            sendAck = sendAck,
            timeout = 10 seconds,
            config = etlConfig.jobMonitor.copy(
              // reconfigure the retries so that resetAfter is
              // exceeded before maxRetries is reached
              retry = RetryConfig(maxRetries = 3, delay = 1 second, resetAfter = 2 seconds)
            )
          )

          // should have updated the job in the database
          val jobState =
            ports.db
              .run(JobsMapper.get(insertedJob.id))
              .awaitFinite()
              .get
              .state

          jobState shouldBe desiredState

          // should have acked the message
          acknowledged.awaitFinite() shouldBe receiptHandle
      }
    }

    "for a deleted package ack the message" in {
      val insertedJob = insertJobAndPayload()

      val (acknowledged, sendAck) = createSendAck

      runStream(
        succeededEcsEvent(insertedJob.id)._1,
        fakePennsieveApiClient(missingPackageSetPackageState),
        sendAck = sendAck
      )

      acknowledged.awaitFinite() shouldBe receiptHandle
    }

    "for a deleted package set the job state to Cancelled" in {
      val insertedJob = insertJobAndPayload()

      runStream(
        succeededEcsEvent(insertedJob.id)._1,
        fakePennsieveApiClient(missingPackageSetPackageState)
      )

      val jobState =
        ports.db
          .run(JobsMapper.get(insertedJob.id))
          .awaitFinite()
          .get
          .state

      jobState shouldBe Cancelled
    }

    "job should be marked completed on success" in {
      val insertedJob = insertJobAndPayload()

      runStream(
        succeededEcsEvent(insertedJob.id)._1,
        fakePennsieveApiClient(successfulSetPackageState)
      )

      val isComplete =
        ports.db
          .run(JobsMapper.get(insertedJob.id))
          .awaitFinite()
          .get
          .completed

      isComplete shouldBe true
    }
  }

  val FiveHoursAhead: OffsetDateTime = OffsetDateTime.now(UTC).plusHours(5L)

  /**
    * Returns a future which will receive the value that is used to call function which is returned as the second argument
    */
  def createTestableFutureFn[M, R](fn: M => R): (Future[M], M => Future[R]) = {
    val promise = Promise[M]()
    promise.future -> { m: M =>
      promise.success(m)
      Future.successful {
        fn(m)
      }
    }
  }

  def createSaveNotification: (Future[MessageBody], SendMessage) =
    createTestableFutureFn(_ => Right(SendMessageResponse.builder().build()))

  def createSendAck: (Future[ReceiptHandle], SendAck) =
    createTestableFutureFn(_ => Right(DeleteMessageResponse.builder.build()))

  private def insertJobAndPayload(payload: Payload = uploadPayload) =
    ports.db
      .run(
        createWithPayload(
          JobId(UUID.randomUUID()),
          payload,
          OrganizationId(organizationId),
          Uploading,
          Some(UserId(userId)),
          taskId = Some(taskId)
        )
      )
      .awaitFinite()

  private def createNotification(
    job: Job,
    payload: Payload,
    success: Boolean = true
  ): NotificationMessage =
    payload match {

      case uploadPayload: Upload =>
        UploadNotification(
          users = List(uploadPayload.userId),
          success = success,
          datasetId = uploadPayload.datasetId,
          packageId = uploadPayload.packageId,
          organizationId = job.organizationId.value,
          uploadedFiles = uploadPayload.files
        )

      case appendPayload: ETLAppendWorkflow =>
        ETLNotification(
          users = List(appendPayload.userId),
          messageType = MessageType.JobDone,
          success = success,
          jobType = appendPayload.`type`,
          importId = job.id.toString,
          organizationId = job.organizationId.value,
          packageId = appendPayload.packageId,
          datasetId = appendPayload.datasetId,
          uploadedFiles = appendPayload.files,
          fileType = appendPayload.fileType,
          packageType = appendPayload.packageType,
          message = s"${appendPayload.`type`} job complete"
        )

      case exportPayload: ETLExportWorkflow =>
        ETLExportNotification(
          users = List(exportPayload.userId),
          messageType = MessageType.JobDone,
          success = success,
          jobType = exportPayload.`type`,
          importId = job.id.toString,
          organizationId = job.organizationId.value,
          packageId = exportPayload.packageId,
          datasetId = exportPayload.datasetId,
          packageType = exportPayload.packageType,
          fileType = exportPayload.fileType,
          sourcePackageId = exportPayload.sourcePackageId,
          sourcePackageType = exportPayload.sourcePackageType,
          message = s"${exportPayload.`type`} job complete"
        )

      case importPayload: ETLWorkflow =>
        ETLNotification(
          users = List(importPayload.userId),
          messageType = MessageType.JobDone,
          success = success,
          jobType = importPayload.`type`,
          importId = job.id.toString,
          organizationId = job.organizationId.value,
          packageId = importPayload.packageId,
          datasetId = importPayload.datasetId,
          uploadedFiles = importPayload.files,
          fileType = importPayload.fileType,
          packageType = importPayload.packageType,
          message = s"${importPayload.`type`} job complete"
        )
    }

  def cloudwatchEvent(jobId: JobId) =
    CloudwatchMessage(
      importId = jobId,
      taskId = TaskId("some-task", "some-cluster"),
      jobState = JobState.Succeeded,
      manifestUri = ManifestUri(etlConfig.s3.etlBucket, jobId),
      sentAt = OffsetDateTime.now(UTC)
    )

  def succeededEcsEvent(jobId: JobId, time: String = OffsetDateTime.now(UTC).toString) = {
    val eventString =
      s"""{
       |  "detail":{
       |    "taskArn":"some-task",
       |    "clusterArn":"some-cluster",
       |    "containers":[
       |      {"exitCode":0}
       |    ],
       |    "overrides":{
       |      "containerOverrides":[
       |        {
       |          "environment":[
       |            {"name":"IMPORT_ID","value":"$jobId"},
       |            {"name":"PARAMS_FILE","value":"${cloudwatchEvent(jobId).manifestUri}"}
       |          ]
       |        }
       |      ]
       |    },
       |    "updatedAt":"$time"
       |  },
       |  "time":"$time"
       |}
      """.stripMargin

    (eventString, JobState.Succeeded)
  }

  def failedEcsEvent(jobId: JobId, time: String = OffsetDateTime.now(UTC).toString) = {
    val eventString =
      s"""{
       |  "detail":{
       |    "taskArn":"some-task",
       |    "clusterArn":"some-cluster",
       |    "containers":[
       |      {"exitCode":1}
       |    ],
       |    "overrides":{
       |      "containerOverrides":[
       |        {
       |          "environment":[
       |            {"name":"IMPORT_ID","value":"$jobId"},
       |            {"name":"PARAMS_FILE","value":"${cloudwatchEvent(jobId).manifestUri}"}
       |          ]
       |        }
       |      ]
       |    },
       |    "updatedAt":"$time"
       |  },
       |  "time":"$time"
       |}
      """.stripMargin

    (eventString, JobState.Failed)
  }

  def pendingEcsEvent(jobId: JobId, time: String = OffsetDateTime.now(UTC).toString) = {
    val eventString =
      s"""{
         |  "detail":{
         |    "taskArn":"some-task",
         |    "clusterArn":"some-cluster",
         |    "containers":[
         |      {"lastStatus":"PENDING"}
         |    ],
         |    "overrides":{
         |      "containerOverrides":[
         |        {
         |          "environment":[
         |            {"name":"IMPORT_ID","value":"$jobId"},
         |            {"name":"PARAMS_FILE","value":"${cloudwatchEvent(jobId).manifestUri}"}
         |          ]
         |        }
         |      ]
         |    },
         |    "updatedAt":"$time"
         |  },
         |  "time":"$time"
         |}
      """.stripMargin

    (eventString, JobState.Pending)
  }

  def runningEcsEvent(jobId: JobId, time: String = OffsetDateTime.now(UTC).toString) = {
    val eventString =
      s"""{
         |  "detail":{
         |    "taskArn":"some-task",
         |    "clusterArn":"some-cluster",
         |    "containers":[
         |      {"lastStatus":"RUNNING"}
         |    ],
         |    "overrides":{
         |      "containerOverrides":[
         |        {
         |          "environment":[
         |            {"name":"IMPORT_ID","value":"$jobId"},
         |            {"name":"PARAMS_FILE","value":"${cloudwatchEvent(jobId).manifestUri}"}
         |          ]
         |        }
         |      ]
         |    },
         |    "updatedAt":"$time"
         |  },
         |  "time":"$time"
         |}
      """.stripMargin

    (eventString, JobState.Running)
  }

  def testMessages(jobId: JobId): List[(String, JobState)] =
    List(
      pendingEcsEvent(jobId),
      runningEcsEvent(jobId),
      succeededEcsEvent(jobId),
      failedEcsEvent(jobId)
    )

  private def runStream(
    cloudwatchEvent: String,
    apiClient: PennsieveApiClient = fakePennsieveApiClient(),
    sendAck: SendAck = successfulAck,
    updateJob: UpdateJob = createUpdateJob(ports.db),
    getJob: GetJob = createGetJob(ports.db),
    notifyJobSource: NotifyJobSource = notifyJobSource,
    getPayload: GetPayload = getPayloadReal,
    getManifest: GetManifest = getManifest,
    timeout: FiniteDuration = 5 seconds,
    config: JobMonitorConfig = etlConfig.jobMonitor
  ) = {
    implicit val jobMonitorPorts: JobMonitorPorts =
      JobMonitorPorts(
        ports.sqsClient,
        getManifest,
        getJob,
        getPayload,
        updateJob,
        notifyJobSource,
        sendAck,
        apiClient
      )

    implicit val jobMonitorConfig: JobMonitorConfig = config

    var state: Option[EitherContext[ETLEvent]] = None

    val finalSink =
      Sink.foreach[EitherContext[ETLEvent]] { maybeState =>
        state = Some(maybeState)
      }

    val jobMonitor =
      new JobMonitor(
        Source.single(
          Message
            .builder()
            .body(cloudwatchEvent)
            .receiptHandle(receiptHandle.value)
            .build()
        ),
        etlConfig.s3.etlBucket
      )

    jobMonitor.run(finalSink).awaitFinite(timeout)

    state
  }
}
