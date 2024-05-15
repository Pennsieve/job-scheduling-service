// Copyright (c) [2018] - [2022] Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.jobscheduling.db

import java.time.OffsetDateTime
import java.time.ZoneOffset.UTC
import java.util.UUID
import com.pennsieve.jobscheduling.TestPayload._
import com.pennsieve.jobscheduling.commons.JobState._
import com.pennsieve.jobscheduling.JobSchedulingServiceSpecHarness
import com.pennsieve.jobscheduling.db.profile.api._
import com.pennsieve.models.JobId
import com.pennsieve.test.AwaitableImplicits
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.BeforeAndAfterEach
import org.scalatest.DoNotDiscover
import org.scalatest.matchers.should.Matchers

@DoNotDiscover
class JobsMapperSpec
    extends AnyWordSpec
    with JobSchedulingServiceSpecHarness
    with BeforeAndAfterEach
    with AwaitableImplicits
    with Matchers {

  override def beforeEach(): Unit = {
    super.beforeEach()

    ports.db.run(OrganizationQuotaMapper.create(1, 10)).awaitFinite()
    ports.db.run(OrganizationQuotaMapper.create(2, 10)).awaitFinite()
    ports.db.run(OrganizationQuotaMapper.create(3, 10)).awaitFinite()
  }

  override def afterEach(): Unit = {
    ports.db.run(TableQuery[OrganizationQuotaTable].delete).awaitFinite()
    ports.db.run(TableQuery[JobsTable].delete).awaitFinite()
    ports.db.run(TableQuery[JobStateTable].delete).awaitFinite()
    super.afterEach()
  }

  "JobsMapper" should {
    "get the next job fairly by org" in {
      insertAvailableJobInDB(1, payload = importPayload(1))
      insertAvailableJobInDB(1, payload = importPayload(2))
      insertAvailableJobInDB(2, payload = importPayload(1))
      insertAvailableJobInDB(2, payload = importPayload(2))
      insertAvailableJobInDB(3, payload = importPayload(1))
      insertAvailableJobInDB(3, payload = importPayload(2))

      val nextOrganizations = (1 to 6).map { x =>
        ports.db.run(JobsMapper.getNextJob()).awaitFinite().get.organizationId
      }

      val expectedNextOrganizations = Vector(1, 2, 3, 1, 2, 3)

      nextOrganizations shouldBe expectedNextOrganizations
    }

    "get the next job fairly by user" in {
      insertAvailableJobInDB(1, 1, payload = importPayload(1, 1))
      insertAvailableJobInDB(1, 1, payload = importPayload(2, 1))
      insertAvailableJobInDB(1, 2, payload = importPayload(3, 2))
      insertAvailableJobInDB(1, 2, payload = importPayload(4, 2))
      insertAvailableJobInDB(1, 3, payload = importPayload(5, 3))
      insertAvailableJobInDB(1, 3, payload = importPayload(6, 3))

      val nextUsers = (1 to 6).map { x =>
        ports.db.run(JobsMapper.getNextJob()).awaitFinite().get.userId.get
      }
      val expectedNextUsers = Vector(1, 2, 3, 1, 2, 3)

      nextUsers shouldBe expectedNextUsers
    }

    "get the next job fairly by user and org" in {
      insertAvailableJobInDB(1, 1, payload = importPayload(1, 1))
      insertAvailableJobInDB(1, 1, payload = importPayload(2, 1))
      insertAvailableJobInDB(1, 2, payload = importPayload(3, 2))
      insertAvailableJobInDB(1, 2, payload = importPayload(4, 2))

      insertAvailableJobInDB(2, 4, payload = importPayload(1, 4))
      insertAvailableJobInDB(2, 4, payload = importPayload(2, 4))
      insertAvailableJobInDB(2, 5, payload = importPayload(3, 5))
      insertAvailableJobInDB(2, 5, payload = importPayload(4, 5))

      insertAvailableJobInDB(3, 7, payload = importPayload(1, 7))
      insertAvailableJobInDB(3, 7, payload = importPayload(2, 7))
      insertAvailableJobInDB(3, 8, payload = importPayload(3, 8))
      insertAvailableJobInDB(3, 8, payload = importPayload(4, 8))

      val nextUserOrgPairs = (1 to 12).map { x =>
        val job = ports.db.run(JobsMapper.getNextJob()).awaitFinite().get
        (job.organizationId, job.userId.get)
      }
      val expectedNextUserOrgPairs = Vector(
        (1, 1),
        (2, 4),
        (3, 7),
        (1, 2),
        (2, 5),
        (3, 8),
        (1, 1),
        (2, 4),
        (3, 7),
        (1, 2),
        (2, 5),
        (3, 8)
      )

      nextUserOrgPairs shouldBe expectedNextUserOrgPairs
    }

    "get the next job fairly by user and org and respect package limitation" in {
      insertAvailableJobInDB(1, 1, payload = importPayload(1, 1))
      insertAvailableJobInDB(1, 1, payload = importPayload(2, 1))
      insertAvailableJobInDB(1, 2, payload = importPayload(3, 2))

      val nextUserOrgPairs = (1 to 3).map { x =>
        val job = ports.db.run(JobsMapper.getNextJob()).awaitFinite().get
        (job.organizationId, job.userId.get)
      }
      val expectedNextUserOrgPairs = Vector((1, 1), (1, 2), (1, 1))

      nextUserOrgPairs shouldBe expectedNextUserOrgPairs
    }

    "only get the next job when package is free" in {
      val job1 = insertAvailableJobInDB(1, payload = importPayload(1))
      val job2 = insertAvailableJobInDB(1, payload = importPayload(1))
      insertAvailableJobInDB(2, payload = importPayload(1))

      val nextOrganizations = (1 to 2).map { x =>
        ports.db.run(JobsMapper.getNextJob()).awaitFinite().get.organizationId
      }

      val expectedNextOrganizations = Vector(1, 2)

      nextOrganizations shouldBe expectedNextOrganizations

      //job2 can not be run since it operates on the same package as job 1
      ports.db.run(JobsMapper.getNextJob()).awaitFinite() shouldBe None

      //we turn job1 to succeeded
      ports.db.run(JobsMapper.updateState(job1.id, Succeeded)).awaitFinite()

      //job2 should be able to run
      ports.db.run(JobsMapper.getNextJob()).awaitFinite().get.id shouldBe job2.id

    }

    "respect organization quotas when scheduling" in {

      ports.db.run(OrganizationQuotaMapper.updateQuota(1, 2)).awaitFinite()
      ports.db.run(OrganizationQuotaMapper.updateQuota(2, 2)).awaitFinite()

      (1 to 50).foreach(c => insertAvailableJobInDB(1, payload = importPayload(c)))
      (1 to 50).foreach(c => insertAvailableJobInDB(2, payload = importPayload(c)))
      (1 to 8).foreach(c => insertAvailableJobInDB(3, payload = importPayload(c)))

      def organizationCounts(jobs: Seq[Job]): Map[Int, Int] =
        jobs.foldLeft(Map.empty[Int, Int])((counts, job) => {
          counts + (job.organizationId -> (counts.getOrElse(job.organizationId, 0) + 1))
        })

      // first six should get distributed evenly
      val firstSix = organizationCounts((1 to 6).flatMap { _ =>
        ports.db.run(JobsMapper.getNextJob()).awaitFinite()
      })
      firstSix(1) shouldBe 2
      firstSix(2) shouldBe 2
      firstSix(3) shouldBe 2

      // next six should all go to org 3 - who has the only available quota
      val nextSix = organizationCounts((1 to 6).flatMap { _ =>
        ports.db.run(JobsMapper.getNextJob()).awaitFinite()
      })
      nextSix.get(1) shouldBe None
      nextSix.get(2) shouldBe None
      nextSix(3) shouldBe 6
    }

    "create a new job" in {
      val entry = ports.db
        .run(PayloadsMapper.create(uploadPayload, Some(uploadPayload.packageId)))
        .awaitFinite()

      val job = ports.db
        .run(
          JobsMapper.create(
            jobId = JobId(UUID.randomUUID()),
            payloadId = entry.id,
            organizationId = organizationId,
            jobState = Uploading,
            userId = Some(userId)
          )
        )
        .awaitFinite()

      ports.db.run(JobsMapper.get(job.id)).awaitFinite() shouldBe Some(job)
    }

    "get the current state of a job" in {
      // estimated starting sentAt
      val sentAt = OffsetDateTime.now(UTC)
      val jobId = insertAvailableJobInDB(1, 3).id

      def getJob = ports.db.run(JobsMapper.get(jobId)).awaitFinite().get

      // insertAvailablejobInDB should have created a job with state 'Available'
      getJob.state shouldBe Available

      // uploading came in after available with an earlier sentAt, so
      // current state should still be available
      ports.db
        .run(JobsMapper.updateState(jobId, Uploading, sentAt.minusMinutes(1)))
        .awaitFinite()
      getJob.state shouldBe Available

      // now the job has been marked as Submitted, current state should
      // be updated accordingly
      ports.db
        .run(
          JobsMapper
            .updateState(jobId, Submitted, TaskId("test", "test"), OffsetDateTime.now(UTC))
        )
        .awaitFinite()

      getJob.state shouldBe Submitted
    }
  }
}
