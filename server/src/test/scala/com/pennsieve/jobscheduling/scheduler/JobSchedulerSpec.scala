// Copyright (c) [2018] - [2022] Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.jobscheduling.scheduler

import akka.actor.ActorSystem
import akka.stream.scaladsl.Sink
import akka.testkit.TestKit
import com.pennsieve.jobscheduling.Fakes.failingUpdateJob
import com.pennsieve.jobscheduling.TestConfig.{
  staticEcsConfig,
  staticJobSchedulerConfig,
  staticS3Config
}
import com.pennsieve.jobscheduling.TestPayload.{
  importPayload,
  insertAvailableJobInDB,
  organizationId
}
import com.pennsieve.jobscheduling.TestTask.taskId
import com.pennsieve.jobscheduling._
import com.pennsieve.jobscheduling.commons.JobState
import com.pennsieve.jobscheduling.db.profile.api._
import com.pennsieve.jobscheduling.db.{ Job, JobsMapper, OrganizationQuotaMapper }
import com.pennsieve.jobscheduling.model.ETLEvent
import com.pennsieve.jobscheduling.model.JobConverters.RichJob
import com.pennsieve.jobscheduling.pusher.JobPusherFakes.{ defaultEcsConfig, defaultPusherConfig }
import com.pennsieve.jobscheduling.pusher.{ JobPusher, JobPusherFakes, JobPusherPorts }
import com.pennsieve.jobscheduling.scheduler.JobSchedulerFakes.{
  runFullSchedulerStream,
  runScheduler,
  schedulerAndRunnableEvent
}
import com.pennsieve.test.AwaitableImplicits
import org.scalatest.BeforeAndAfterEach
import org.scalatest.DoNotDiscover
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.duration.DurationLong

@DoNotDiscover
class JobSchedulerSpec(system: ActorSystem)
    extends TestKit(system)
    with AnyWordSpecLike
    with JobSchedulingServiceSpecHarness
    with AwaitableImplicits
    with Matchers
    with BeforeAndAfterEach {

  def this() = this(ActorSystem("JobSchedulerSpec"))

  implicit val actorSystem = system
  implicit val scheduler = system.scheduler

  override def beforeEach(): Unit = {
    super.beforeEach()
    ports.db.run(JobsMapper.delete).awaitFinite()
    ports.db.run(OrganizationQuotaMapper.delete).awaitFinite()
    ports.db.run(OrganizationQuotaMapper.create(organizationId, 10)).awaitFinite()
  }

  val etlBucket: String = staticS3Config.etlBucket

  "Source" should {

    "should work just like the plain query" in {

      ports.db.run(OrganizationQuotaMapper.create(2, 10)).awaitFinite()
      ports.db.run(OrganizationQuotaMapper.create(3, 10)).awaitFinite()

      insertAvailableJobInDB(1, 3, payload = importPayload(1, 3))
      insertAvailableJobInDB(1, 4, payload = importPayload(2, 4))
      insertAvailableJobInDB(1, 5, payload = importPayload(3, 5))
      insertAvailableJobInDB(1, 6, payload = importPayload(4, 6))
      insertAvailableJobInDB(1, 7, payload = importPayload(5, 7))
      insertAvailableJobInDB(2, 8, payload = importPayload(6, 8))
      insertAvailableJobInDB(2, 9, payload = importPayload(7, 9))
      insertAvailableJobInDB(2, 10, payload = importPayload(8, 10))
      insertAvailableJobInDB(2, 11, payload = importPayload(9, 11))
      insertAvailableJobInDB(2, 12, payload = importPayload(10, 12))
      insertAvailableJobInDB(3, 13, payload = importPayload(11, 13))
      insertAvailableJobInDB(3, 14, payload = importPayload(12, 14))
      insertAvailableJobInDB(3, 15, payload = importPayload(13, 15))
      insertAvailableJobInDB(3, 16, payload = importPayload(14, 16))
      insertAvailableJobInDB(3, 17, payload = importPayload(15, 17))

      val fakeFargate = new FakeFargate(0)

      val (jobScheduler, _) = schedulerAndRunnableEvent(fakeFargate.runTask())

      val src = jobScheduler.source

      val result: immutable.Seq[Option[Job]] =
        Future
          .sequence(
            src
              .take(9)
              .runWith(Sink.seq)
              .awaitFinite(10 seconds)
              .map(event => ports.db.run(JobsMapper.get(event.importId)))
          )
          .awaitFinite(10 seconds)

      val sched: immutable.Seq[Job] = result.take(3).flatten

      //the first 3 jobs should include all three organization
      sched.map(_.organizationId).toSet shouldBe Set(organizationId, 2, 3)

      val nextSched = result.slice(3, 9).flatten

      val users = nextSched.flatMap(_.userId).toSet.size

      users shouldBe 6 //6 jobs should be distributed amongst 6 users
    }

  }

  "Scheduler" should {
    "successfully start a task" in {
      val fakeFargate = new FakeFargate(0)

      val job = insertAvailableJobInDB(organizationId)

      val (_, eventualEvent) = runScheduler(fakeFargate.runTask())
      ports.db.run(JobsMapper.get(job.id)).awaitFinite().value

      val event = eventualEvent.awaitFinite()

      event shouldBe someSuccessfulEvent(job, event)

      fakeFargate.getSuccessfulRuns shouldBe 1
    }

    "successfully start a task when it is added" in {
      val fakeFargate = new FakeFargate(0)

      val job = insertAvailableJobInDB(organizationId)

      val (jobScheduler, eventualEvent) = runScheduler(fakeFargate.runTask())

      jobScheduler.addJob().awaitFinite() shouldBe Right(JobQueued)

      ports.db.run(JobsMapper.get(job.id)).awaitFinite().value

      val event = eventualEvent.awaitFinite(10 seconds)
      event shouldBe someSuccessfulEvent(job, event)

      fakeFargate.getSuccessfulRuns shouldBe 1
    }

    "shutdown queue when Scheduler shuts down" in {
      val jobSchedulerPorts =
        JobSchedulerPorts(
          () => ports.db.run(JobsMapper.getNextJob()),
          JobSchedulerFakes.emptyListTasks,
          pusher = new JobPusher(
            JobPusherPorts(
              JobPusherFakes.successfulRunTask,
              JobPusherFakes.stubManifestPut,
              JobSchedulingPorts.createGetPayload(ports.db)(_)
            ),
            defaultPusherConfig,
            staticS3Config.etlBucket,
            defaultEcsConfig
          ),
          JobSchedulingPorts.createUpdateJob(ports.db)
        )

      val jobScheduler =
        new JobScheduler(staticEcsConfig, staticJobSchedulerConfig, jobSchedulerPorts)

      jobScheduler.source.runWith(Sink.cancelled)

      jobScheduler.closeQueue

      jobScheduler.addJob().awaitFinite() shouldBe Left(JobNotQueued)
    }

    "not fail a job if above task limit" in {
      val fakeFargate = new FakeFargate(attemptsTilSuccess = 2)

      val job = insertAvailableJobInDB(organizationId)

      val (_, eventualEvent) = runScheduler(listTasks = fakeFargate.listTasks)

      val event = eventualEvent.awaitFinite()

      event shouldBe someSuccessfulEvent(job, event)
    }

    "not fail if list tasks attempt throws" in {
      val fakeFargate = new FakeFargate()

      val job = insertAvailableJobInDB(organizationId)

      val (_, eventualEvent) = runScheduler(listTasks = fakeFargate.throwingListTasks)

      val event = eventualEvent.awaitFinite()

      event shouldBe someSuccessfulEvent(job, event)
    }

    "re-run the stream on failure" in {
      // the first of these will fail and end up stuck in 'SUBMITTED',
      // the second will get picked up when the stream restarts
      val (failingJob, successfulJob) =
        (
          insertAvailableJobInDB(organizationId, payload = importPayload(1)),
          insertAvailableJobInDB(organizationId, payload = importPayload(2))
        )

      val event = runFullSchedulerStream(updateJob = Some(failingUpdateJob(ports.db, 1)))

      event.isEmpty shouldBe false
      event.get.isRight shouldBe true

      // successful job should have gotten picked up
      event.get.toOption shouldBe someSuccessfulEvent(successfulJob, event.get.toOption)

      // failing job should be stuck in submitted
      val jobState =
        ports.db
          .run(JobsMapper.get(failingJob.id))
          .awaitFinite()
          .get
          .state

      jobState shouldBe JobState.Submitted
    }
  }

  private def someSuccessfulEvent(job: Job, event: Option[ETLEvent]) =
    Some(job.toSuccessfulEvent(etlBucket, taskId, JobState.Submitted, sentAt = event.get.sentAt))
}
