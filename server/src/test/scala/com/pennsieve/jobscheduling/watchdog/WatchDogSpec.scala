// Copyright (c) [2018] - [2022] Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.jobscheduling.watchdog

import java.time.{ Instant, OffsetDateTime }
import java.time.ZoneOffset.UTC
import java.util.Date
import akka.Done
import akka.actor.{ ActorSystem, Scheduler }
import akka.stream.scaladsl.{ Keep, Sink, Source }
import akka.testkit.TestKit
import software.amazon.awssdk.services.ecs.model.{
  ContainerOverride,
  KeyValuePair,
  Task,
  TaskOverride
}
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesResponse
import software.amazon.awssdk.services.sqs.model.QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES
import com.pennsieve.jobscheduling.Fakes.getManifest
import com.pennsieve.jobscheduling.JobSchedulingPorts.{
  createGetJob,
  createGetPayload,
  createUpdateJob,
  GetJob
}
import com.pennsieve.jobscheduling.TestConfig.{ staticS3Config, staticWatchDogConfig }
import com.pennsieve.jobscheduling.TestPayload._
import com.pennsieve.jobscheduling.TestTask._
import com.pennsieve.jobscheduling._
import com.pennsieve.jobscheduling.clients.SQSClient.{ GetNumberOfMessages, QueueName }
import com.pennsieve.jobscheduling.commons.JobState
import com.pennsieve.jobscheduling.commons.JobState.{ Sent, Uploading }
import com.pennsieve.jobscheduling.db.JobsMapper.update
import com.pennsieve.jobscheduling.db._
import com.pennsieve.jobscheduling.db.profile.api._
import com.pennsieve.jobscheduling.handlers.JobsHandlerPorts.NotifyUpload
import com.pennsieve.jobscheduling.model.EventualResult.EitherContext
import com.pennsieve.jobscheduling.model.JobConverters.{
  payloadToManifest,
  ImportId,
  JobSchedulingService,
  PayloadId,
  RichJob
}
import com.pennsieve.jobscheduling.model._
import com.pennsieve.jobscheduling.scheduler.JobQueued
import com.pennsieve.jobscheduling.scheduler.JobSchedulerFakes.emptyDescribeTasks
import com.pennsieve.jobscheduling.watchdog.JobStateWatchDogPorts.createGetJobsStuckInState
import com.pennsieve.jobscheduling.watchdog.WatchDogPorts._
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatest.EitherValues._

import scala.concurrent.Future
import scala.jdk.CollectionConverters._

class WatchDogSpec(system: ActorSystem)
    extends TestKit(system)
    with AnyWordSpecLike
    with JobSchedulingServiceSpecHarness
    with Matchers
    with BeforeAndAfterEach {

  def this() = this(ActorSystem("WatchDogSpec"))

  implicit val actorSystem: ActorSystem = system
  implicit val scheduler: Scheduler = system.scheduler
  implicit val config = staticWatchDogConfig

  val exception = new Exception()
  val etlBucket: String = staticS3Config.etlBucket
  val yesterday: OffsetDateTime = OffsetDateTime.now(UTC).minusDays(1L)
  val yesterdayInstant = yesterday.toInstant
  val yesterdayDate: Date = new Date(yesterday.toInstant.toEpochMilli)

  override def beforeEach(): Unit = {
    super.beforeEach()
    ports.db.run(JobsMapper.delete).awaitFinite()
  }

  "A watchdog" should {
    "stop jobs that have been running for more than half a day" in {
      val fakeFargate = new FakeFargate(0)

      val slowJob = createJob(Some(taskId))
      val correspondingTask = createOldInactiveTaskWithEnv(entryToJob(slowJob))

      ports.db.run(update(slowJob, createJobState(slowJob.id), uploadPayload)).awaitFinite()

      val event = runWatchDogDatabase(
        fakeFargate.stopTask,
        describeTasks = fakeFargate.describeTasks(correspondingTask)
      )
      val expectedEvent = entryToJob(slowJob).toSuccessfulEvent(
        etlBucket,
        taskId,
        jobState = JobState.Failed,
        sentAt = event.get.toOption.get.sentAt
      )

      event shouldBe Some(Right(expectedEvent))
      fakeFargate.stoppedTask shouldBe Some(taskId.taskArn)
    }

    "create an event if a task cannot be found for a job" in {
      val fakeFargate = new FakeFargate(0)

      val jobWithoutTaskId = createJob(None)
      val jobId = jobWithoutTaskId.id

      ports.db
        .run(update(jobWithoutTaskId, createJobState(jobWithoutTaskId.id), uploadPayload))
        .awaitFinite()

      val event = runWatchDogDatabase(fakeFargate.stopTask)
      val someLostTaskEvent =
        Some(
          Right(
            FailedLostTaskEvent(
              jobId,
              ManifestUri(etlBucket, jobId),
              jobWithoutTaskId.organizationId,
              jobWithoutTaskId.userId,
              Some(jobWithoutTaskId.payloadId),
              sentAt = event.get.toOption.get.sentAt
            )
          )
        )

      event shouldBe someLostTaskEvent

      fakeFargate.stoppedTask shouldBe None
    }

    "stop a task for a job without a task id if the task contains a job id" in {
      val fakeFargate = new FakeFargate(0)

      val jobNoTaskId = createJob(None)

      ports.db.run(update(jobNoTaskId, createJobState(jobNoTaskId.id), uploadPayload)).awaitFinite()

      val taskWithJobId = createOldInactiveTaskWithEnv(entryToJob(jobNoTaskId))

      val event =
        runWatchDogDatabase(fakeFargate.stopTask, fakeFargate.describeTasks(taskWithJobId))
      val expectedEvent = entryToJob(jobNoTaskId).toSuccessfulEvent(
        etlBucket,
        taskId,
        jobState = JobState.Failed,
        sentAt = event.get.toOption.get.sentAt
      )

      event shouldBe Some(Right(expectedEvent))

      fakeFargate.stoppedTask shouldBe Some(taskId.taskArn)
    }

    "stop a task that cannot be linked to a job in the database" in {
      val fakeFargate = new FakeFargate(0)

      val slowJob = createJob(Some(taskId))

      val taskWithJobId = createOldInactiveTaskWithEnv(entryToJob(slowJob))

      runWatchDogECS(fakeFargate.stopTask, fakeFargate.describeTasks(taskWithJobId))

      fakeFargate.stoppedTask shouldBe Some(taskId.taskArn)
    }

    "stop a task that cannot be linked to a job due to a failing database" in {
      val fakeFargate = new FakeFargate(0)

      val slowJob = createJob(Some(taskId))

      val taskWithJobId = createOldInactiveTaskWithEnv(entryToJob(slowJob))

      runWatchDogECS(
        fakeFargate.stopTask,
        fakeFargate.describeTasks(taskWithJobId),
        getJob = _ => Future.failed(exception)
      )

      fakeFargate.stoppedTask shouldBe Some(taskId.taskArn)
    }

    "not stop a task that is still running within expected time period" in {
      val fakeFargate = new FakeFargate(0)

      val regularJob = createJob(Some(taskId))

      val taskWithJobId =
        initOldInactiveTaskBuilderWithEnv(entryToJob(regularJob)).createdAt(Instant.now()).build()

      runWatchDogECS(fakeFargate.stopTask, fakeFargate.describeTasks(taskWithJobId))

      fakeFargate.stoppedTask shouldBe None
    }

    "not stop a task that is still running within expected time period where database is down" in {
      val fakeFargate = new FakeFargate(0)

      val regularJob = createJob(Some(taskId))

      val taskWithJobId =
        initOldInactiveTaskBuilderWithEnv(entryToJob(regularJob)).createdAt(Instant.now()).build()

      runWatchDogECS(
        fakeFargate.stopTask,
        fakeFargate.describeTasks(taskWithJobId),
        getJob = _ => Future.failed(exception)
      )

      fakeFargate.stoppedTask shouldBe None
    }

    "continue to stop jobs for a completely failing database" in {
      val fakeFargate = new FakeFargate(0)

      val slowJob = createJob(Some(taskId))

      val taskWithJobId =
        initOldInactiveTaskBuilderWithEnv(entryToJob(slowJob)).createdAt(yesterdayInstant).build()

      runWatchDogECS(
        fakeFargate.stopTask,
        fakeFargate.describeTasks(taskWithJobId),
        getJob = _ => Future.failed(exception),
        getOldActiveJobs = _ => Future.failed(exception)
      )

      fakeFargate.stoppedTask shouldBe Some(taskId.taskArn)
    }

    "not stop a task not started by etl service" in {
      val fakeFargate = new FakeFargate(0)

      val regularJob = createJob(Some(taskId))

      val taskWithJobId =
        initOldInactiveTaskBuilderWithEnv(entryToJob(regularJob))
          .startedBy("not-etl-service")
          .build()

      runWatchDogECS(fakeFargate.stopTask, fakeFargate.describeTasks(taskWithJobId))

      fakeFargate.stoppedTask shouldBe None
    }

    "not stop an active task" in {
      val fakeFargate = new FakeFargate(0)

      val regularJob = createJob(Some(taskId))

      val taskWithJobId =
        initOldInactiveTaskBuilderWithEnv(entryToJob(regularJob)).desiredStatus("RUNNING").build()

      runWatchDogECS(fakeFargate.stopTask, fakeFargate.describeTasks(taskWithJobId))

      fakeFargate.stoppedTask shouldBe None
    }

    "re-run stream on failure" in {
      val fakeFargate = new FakeFargate(2)

      val slowJob = createJob(Some(taskId))
      val correspondingTask = createOldInactiveTaskWithEnv(entryToJob(slowJob))

      ports.db.run(update(slowJob, createJobState(slowJob.id), uploadPayload)).awaitFinite()

      runFullWatchDogStream(
        fakeFargate.failingStopTask,
        describeTasks = fakeFargate.successfulDescribeTasks(correspondingTask)
      )

      fakeFargate.stoppedTask shouldBe Some(taskId.taskArn)
    }
  }

  "JobStateWatchDog" should {
    "resend a job stuck in state uploading" in {
      val fakeSQS = new FakeSQS()

      val jobRecord = createJob(None)

      val state = createOldJobState(jobRecord.id, Uploading)

      ports.db.run(update(jobRecord, state, uploadPayload)).awaitFinite()

      val job: Job = ports.db.run(JobsMapper.get(jobRecord.id)).awaitFinite().get

      val payloadEntry = ports.db.run(PayloadsMapper.get(job.payloadId)).awaitFinite()
      val payload = payloadEntry.map(_.payload)

      runJobStateWatchDog(fakeSQS.savingManifestUploadNotifier)

      fakeSQS.sentManifest shouldBe Some(payloadToManifest(job)(payload).value)
    }

    "not resend if there are less jobs stuck in uploading than in SQS waiting" in {
      val fakeSQS = new FakeSQS()

      val jobRecord = createJob(None)

      val state = createOldJobState(jobRecord.id, Uploading)

      ports.db.run(update(jobRecord, state, uploadPayload)).awaitFinite()

      runJobStateWatchDog(fakeSQS.savingManifestUploadNotifier, successfulGetNumberOfMessages(1))

      fakeSQS.sentManifest shouldBe None
    }

    "resend a job up to a configured maximum number of times" in {
      val fakeSQS = new FakeSQS()

      val jobRecord = createJob(None)

      ports.db
        .run(update(jobRecord, createOldJobState(jobRecord.id, Uploading), uploadPayload))
        .awaitFinite()
      ports.db
        .run(update(jobRecord, createOldJobState(jobRecord.id, Uploading), uploadPayload))
        .awaitFinite()
      ports.db
        .run(update(jobRecord, createOldJobState(jobRecord.id, Uploading), uploadPayload))
        .awaitFinite()

      ports.db
        .run(JobsMapper.getStateRetries(jobRecord.id, Uploading))
        .awaitFinite() shouldBe 3

      runJobStateWatchDog(fakeSQS.savingManifestUploadNotifier)

      fakeSQS.sentManifest shouldBe None
    }

    "should not retry defunct states" in {
      val fakeSQS = new FakeSQS()

      val jobRecord = createJob(None)

      ports.db
        .run(update(jobRecord, createOldJobState(jobRecord.id, Uploading), uploadPayload))
        .awaitFinite()
      ports.db
        .run(update(jobRecord, createOldJobState(jobRecord.id, Sent), importPayload()))
        .awaitFinite()

      runJobStateWatchDog(fakeSQS.savingManifestUploadNotifier)

      fakeSQS.sentManifest shouldBe None
    }
  }

  private def initOldInactiveTaskBuilderWithEnv(slowJob: Job): Task.Builder = {
    val envJobId =
      KeyValuePair.builder().name(ImportId).value(slowJob.id.toString).build()
    val envPayloadId =
      KeyValuePair
        .builder()
        .name(PayloadId)
        .value(slowJob.payloadId.toString)
        .build()
    val containerOverride =
      ContainerOverride
        .builder()
        .environment(List(envPayloadId, envJobId).asJava)
        .build()
    val taskOverride =
      TaskOverride.builder().containerOverrides(containerOverride).build()
    initTaskBuilder()
      .overrides(taskOverride)
      .startedBy(JobSchedulingService)
      .createdAt(yesterdayInstant)
      .desiredStatus("STOPPED")
  }

  private def createOldInactiveTaskWithEnv(slowJob: Job) = {
    initOldInactiveTaskBuilderWithEnv(slowJob).build()
  }

  def runWatchDogDatabase(
    stopTask: StopTask,
    describeTasks: DescribeTasks = emptyDescribeTasks,
    getJob: GetJob = createGetJob(ports.db),
    getOldActiveJobs: GetOldActiveJobs = createGetOldActiveJobs(ports.db)
  ): Option[EitherContext[ETLEvent]] = {
    var state: Option[EitherContext[ETLEvent]] = None

    val finalSink = Sink.foreach[EitherContext[ETLEvent]] { maybeState =>
      state = Some(maybeState)
    }

    implicit val watchDogPorts: WatchDogPorts =
      WatchDogPorts(
        getOldActiveJobs = createGetOldActiveJobs(ports.db),
        stopTask = stopTask,
        describeTasks = describeTasks,
        getManifest = getManifest,
        getJob = getJob,
        getPayload = createGetPayload(ports.db),
        updateJob = createUpdateJob(ports.db),
        notifyJobSource = () => Future.successful(Right(JobQueued)),
        pennsieveApiClient = Fakes.fakePennsieveApiClient()
      )

    Source
      .single(Tick)
      .runWith(JobWatchDog(etlBucket, clusterArn).toMat(finalSink)(Keep.right))
      .awaitFinite()

    state
  }

  def runWatchDogECS(
    stopTask: StopTask,
    describeTasks: DescribeTasks = emptyDescribeTasks,
    getJob: GetJob = createGetJob(ports.db),
    getOldActiveJobs: GetOldActiveJobs = createGetOldActiveJobs(ports.db)
  ): Done = {
    implicit val watchDogPorts: WatchDogPorts =
      WatchDogPorts(
        getOldActiveJobs = createGetOldActiveJobs(ports.db),
        stopTask = stopTask,
        describeTasks = describeTasks,
        getManifest = getManifest,
        getJob = getJob,
        getPayload = createGetPayload(ports.db),
        updateJob = createUpdateJob(ports.db),
        notifyJobSource = () => Future.successful(Right(JobQueued)),
        pennsieveApiClient = Fakes.fakePennsieveApiClient()
      )

    Source
      .single(Tick)
      .runWith(TaskWatchDog(config, taskId.clusterArn))
      .awaitFinite()
  }

  def successfulGetNumberOfMessages(numberOfMessages: Int): GetNumberOfMessages =
    () =>
      Future.successful(
        Right(
          GetQueueAttributesResponse
            .builder()
            .attributes(Map(APPROXIMATE_NUMBER_OF_MESSAGES -> numberOfMessages.toString).asJava)
            .build()
        )
      )

  def runJobStateWatchDog(
    notifyUpload: NotifyUpload,
    getNumberOfMessages: GetNumberOfMessages = successfulGetNumberOfMessages(0)
  ): Done = {
    val jobStateWatchDogPorts =
      JobStateWatchDogPorts(
        notifyUpload,
        createGetJobsStuckInState(ports.db, config.jobState.maxRetries),
        createGetPayload(ports.db),
        getNumberOfMessages
      )

    Source
      .single(Tick)
      .runWith(JobStateWatchDog(config.jobState, jobStateWatchDogPorts))
      .awaitFinite()
  }

  def runFullWatchDogStream(
    stopTask: StopTask,
    describeTasks: DescribeTasks = emptyDescribeTasks,
    getJob: GetJob = createGetJob(ports.db),
    getOldActiveJobs: GetOldActiveJobs = createGetOldActiveJobs(ports.db)
  ) = {
    implicit val watchDogPorts: WatchDogPorts =
      WatchDogPorts(
        getOldActiveJobs = createGetOldActiveJobs(ports.db),
        stopTask = stopTask,
        describeTasks = describeTasks,
        getManifest = getManifest,
        getJob = getJob,
        getPayload = createGetPayload(ports.db),
        updateJob = createUpdateJob(ports.db),
        notifyJobSource = () => Future.successful(Right(JobQueued)),
        pennsieveApiClient = Fakes.fakePennsieveApiClient()
      )

    var state: Option[EitherContext[ETLEvent]] = None

    val finalSink = Sink.foreach[EitherContext[ETLEvent]] { maybeState =>
      state = Some(maybeState)
    }

    new WatchDog(clusterArn, etlBucket, QueueName("upload-consumer"), config, watchDogPorts)
      .run(finalSink, source = Source.single(Tick))
      .awaitFinite()

    state
  }
}
