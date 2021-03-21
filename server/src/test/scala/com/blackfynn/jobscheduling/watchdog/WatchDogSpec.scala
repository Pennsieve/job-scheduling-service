// Copyright (c) [2018] - [2020] Pennsieve, Inc. All Rights Reserved.

package com.blackfynn.jobscheduling.watchdog

import java.time.OffsetDateTime
import java.time.ZoneOffset.UTC
import java.util.Date

import akka.Done
import akka.actor.{ ActorSystem, Scheduler }
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{ Keep, Sink, Source }
import akka.testkit.TestKit
import com.amazonaws.services.ecs.model.{ ContainerOverride, KeyValuePair, TaskOverride }
import com.amazonaws.services.sqs.model.GetQueueAttributesResult
import com.blackfynn.jobscheduling.Fakes.getManifest
import com.blackfynn.jobscheduling.JobSchedulingPorts.{
  createGetJob,
  createGetPayload,
  createUpdateJob,
  GetJob
}
import com.blackfynn.jobscheduling.TestConfig.{ staticS3Config, staticWatchDogConfig }
import com.blackfynn.jobscheduling.TestPayload._
import com.blackfynn.jobscheduling.TestTask._
import com.blackfynn.jobscheduling._
import com.blackfynn.jobscheduling.clients.SQSClient.{ GetNumberOfMessages, QueueName }
import com.blackfynn.jobscheduling.commons.JobState
import com.blackfynn.jobscheduling.commons.JobState.{ Sent, Uploading }
import com.blackfynn.jobscheduling.db.JobsMapper.update
import com.blackfynn.jobscheduling.db._
import com.blackfynn.jobscheduling.db.profile.api._
import com.blackfynn.jobscheduling.handlers.JobsHandlerPorts.NotifyUpload
import com.blackfynn.jobscheduling.model.EventualResult.EitherContext
import com.blackfynn.jobscheduling.model.JobConverters.{
  payloadToManifest,
  ImportId,
  JobSchedulingService,
  PayloadId,
  RichJob
}
import com.blackfynn.jobscheduling.model._
import com.blackfynn.jobscheduling.scheduler.JobQueued
import com.blackfynn.jobscheduling.scheduler.JobSchedulerFakes.emptyDescribeTasks
import com.blackfynn.jobscheduling.watchdog.JobStateWatchDogPorts.createGetJobsStuckInState
import com.blackfynn.jobscheduling.watchdog.WatchDogPorts._
import org.scalatest.{ BeforeAndAfterEach, Matchers, WordSpecLike }

import scala.concurrent.Future

class WatchDogSpec(system: ActorSystem)
    extends TestKit(system)
    with WordSpecLike
    with JobSchedulingServiceSpecHarness
    with Matchers
    with BeforeAndAfterEach {

  def this() = this(ActorSystem("WatchDogSpec"))

  implicit val actorSystem: ActorSystem = system
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val scheduler: Scheduler = system.scheduler

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
        createOldInactiveTaskWithEnv(entryToJob(regularJob)).withCreatedAt(new Date())

      runWatchDogECS(fakeFargate.stopTask, fakeFargate.describeTasks(taskWithJobId))

      fakeFargate.stoppedTask shouldBe None
    }

    "not stop a task that is still running within expected time period where database is down" in {
      val fakeFargate = new FakeFargate(0)

      val regularJob = createJob(Some(taskId))

      val taskWithJobId =
        createOldInactiveTaskWithEnv(entryToJob(regularJob)).withCreatedAt(new Date())

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
        createOldInactiveTaskWithEnv(entryToJob(slowJob)).withCreatedAt(yesterdayDate)

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
        createOldInactiveTaskWithEnv(entryToJob(regularJob)).withStartedBy("not-etl-service")

      runWatchDogECS(fakeFargate.stopTask, fakeFargate.describeTasks(taskWithJobId))

      fakeFargate.stoppedTask shouldBe None
    }

    "not stop an active task" in {
      val fakeFargate = new FakeFargate(0)

      val regularJob = createJob(Some(taskId))

      val taskWithJobId =
        createOldInactiveTaskWithEnv(entryToJob(regularJob)).withDesiredStatus("RUNNING")

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

      fakeSQS.sentManifest shouldBe Some(payloadToManifest(job)(payload).right.get)
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

  private def createOldInactiveTaskWithEnv(slowJob: Job) = {
    import scala.collection.JavaConverters._
    val envJobId =
      new KeyValuePair().withName(ImportId).withValue(slowJob.id.toString)
    val envPayloadId =
      new KeyValuePair()
        .withName(PayloadId)
        .withValue(slowJob.payloadId.toString)
    val containerOverride =
      new ContainerOverride()
        .withEnvironment(List(envPayloadId, envJobId).asJava)
    val taskOverride =
      new TaskOverride().withContainerOverrides(containerOverride)
    createTask()
      .withOverrides(taskOverride)
      .withStartedBy(JobSchedulingService)
      .withCreatedAt(yesterdayDate)
      .withDesiredStatus("STOPPED")
  }

  val exception = new Exception()
  val etlBucket: String = staticS3Config.etlBucket
  val yesterday: OffsetDateTime = OffsetDateTime.now(UTC).minusDays(1L)
  val yesterdayDate: Date = new Date(yesterday.toInstant.toEpochMilli)

  implicit val config = staticWatchDogConfig

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

  import akka.stream.alpakka.sqs.ApproximateNumberOfMessages
  def successfulGetNumberOfMessages(numberOfMessages: Int): GetNumberOfMessages =
    () =>
      Future.successful(
        Right(
          new GetQueueAttributesResult()
            .addAttributesEntry(ApproximateNumberOfMessages.name, numberOfMessages.toString)
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
