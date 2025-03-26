// Copyright (c) [2018] - [2025] Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.jobscheduling.db

import java.sql.Timestamp
import java.time.OffsetDateTime
import java.time.ZoneOffset.UTC
import java.util.UUID

import com.pennsieve.auth.middleware.{ OrganizationId, UserId }
import com.pennsieve.jobscheduling.ETLLogContext
import com.pennsieve.jobscheduling.commons.JobState
import com.pennsieve.jobscheduling.db.profile.api._
import com.pennsieve.jobscheduling.errors.NoJobException
import com.pennsieve.jobscheduling.model.Cursor
import com.pennsieve.models.{ JobId, Payload }
import io.circe.generic.semiauto.{ deriveDecoder, deriveEncoder }
import io.circe.{ Decoder, Encoder, Json }
import slick.dbio.{ DBIOAction, Effect, NoStream }
import slick.jdbc.{ GetResult, TransactionIsolation }
import slick.lifted.MappedProjection
import slick.sql.{ FixedSqlStreamingAction, SqlAction }

import scala.concurrent.ExecutionContext

/*
 * A 'job' - the result of merging a JobRecord with the latest
 * JobStateRecord for that job
 */
final case class Job(
  id: JobId,
  payloadId: Int,
  organizationId: Int,
  userId: Option[Int],
  state: JobState,
  completed: Boolean = false,
  taskId: Option[TaskId] = None,
  submittedAt: Option[OffsetDateTime] = None,
  createdAt: OffsetDateTime = OffsetDateTime.now(UTC),
  updatedAt: OffsetDateTime = OffsetDateTime.now(UTC)
) {
  val logContext: ETLLogContext =
    ETLLogContext(Some(this.id), Some(this.organizationId), this.userId)
}

object Job {

  implicit val encoder: Encoder[Job] = deriveEncoder[Job]
  implicit val decoder: Decoder[Job] = deriveDecoder[Job]

  // to fix slick bug: https://github.com/slick/slick/issues/1873#issuecomment-395741809
  def tupled = (Job.apply _).tupled
}

/*
 * A record in the `jobs` table
 */
final case class JobRecord(
  id: JobId,
  payloadId: Int,
  organizationId: Int,
  userId: Option[Int],
  completed: Boolean,
  taskId: Option[TaskId] = None,
  submittedAt: Option[OffsetDateTime] = None,
  createdAt: OffsetDateTime = OffsetDateTime.now(UTC),
  updatedAt: OffsetDateTime = OffsetDateTime.now(UTC)
)

/*
 * The main jobs table.
 */
final class JobsTable(tag: Tag) extends Table[JobRecord](tag, Some(schema), "jobs") {

  def payloadId = column[Int]("payload_id")
  def organizationId = column[Int]("organization_id")
  def userId = column[Option[Int]]("user_id")
  def completed = column[Boolean]("completed")
  def taskId = column[Option[TaskId]]("task_id")

  def submittedAt = column[Option[OffsetDateTime]]("submitted_at")

  def createdAt = column[OffsetDateTime]("created_at")
  def updatedAt = column[OffsetDateTime]("updated_at")
  def id = column[JobId]("id", O.PrimaryKey)

  def * =
    (id, payloadId, organizationId, userId, completed, taskId, submittedAt, createdAt, updatedAt)
      .mapTo[JobRecord]
}

object JobsMapper extends TableQuery(new JobsTable(_)) {
  private val state = JobStateMapper

  def withCurrentState(
    base: Query[JobsTable, JobRecord, Seq] = this
  ): Query[(JobsTable, Rep[JobState]), (JobRecord, JobState), Seq] =
    (base join state.currentState on ((j, s) => j.id === s._1))
      .map { case (job, state) => (job, state._2) }

  def withCurrentStateNotCompleted(
    base: Query[JobsTable, JobRecord, Seq] = this
  ): Query[(JobsTable, Rep[JobState]), (JobRecord, JobState), Seq] =
    withCurrentState(base.filter(_.completed === false))

  def withAllStates(
    base: Query[JobsTable, JobRecord, Seq] = this
  ): Query[(JobsTable, JobStateTable), (JobRecord, JobStateRecord), Seq] =
    base join state on ((j, s) => j.id === s.jobId)

  def withAllStatesNotCompleted(
    base: Query[JobsTable, JobRecord, Seq] = this
  ): Query[(JobsTable, JobStateTable), (JobRecord, JobStateRecord), Seq] =
    withAllStates(base.filter(_.completed === false))

  /*
   * Combine a job record and a state record to form a Job object
   */
  private def combineJobStateRecord(jobAndState: (JobRecord, JobStateRecord)): Job =
    jobAndState match {
      case (jobRecord, stateRecord) =>
        Job(
          jobRecord.id,
          jobRecord.payloadId,
          jobRecord.organizationId,
          jobRecord.userId,
          stateRecord.state,
          jobRecord.completed,
          jobRecord.taskId,
          jobRecord.submittedAt,
          jobRecord.createdAt,
          jobRecord.updatedAt
        )
    }

  private def toJob(jobAndState: (JobsTable, Rep[JobState])): MappedProjection[
    Job,
    (
      JobId,
      Int,
      Int,
      Option[Int],
      JobState,
      Boolean,
      Option[TaskId],
      Option[OffsetDateTime],
      OffsetDateTime,
      OffsetDateTime
    )
  ] = {
    val (job, state) = jobAndState
    (
      job.id,
      job.payloadId,
      job.organizationId,
      job.userId,
      state,
      job.completed,
      job.taskId,
      job.submittedAt,
      job.createdAt,
      job.updatedAt
    ).mapTo[Job]
  }

  /*
   * Get a job object corresponding to the given JobId, along with its
   * current from the job_state table
   */
  def get(id: JobId): SqlAction[Option[Job], NoStream, Effect.Read] =
    withCurrentState(this.filter(_.id === id))
      .map(toJob)
      .result
      .headOption

  def updatePayload(
    id: JobId,
    payload: Payload
  )(implicit
    executionContext: ExecutionContext
  ) = {
    for {
      payloadIdOption <- this
        .filter(_.id === id)
        .map(_.payloadId)
        .result
        .headOption
      _ <- payloadIdOption match {
        case Some(payloadId) =>
          PayloadsMapper
            .filter(_.id === payloadId)
            .map(_.payload)
            .update(payload)
        case None => DBIO.failed(NoJobException(id))
      }
    } yield ()
  }

  def create(
    jobId: JobId,
    payloadId: Int,
    organizationId: Int,
    jobState: JobState,
    userId: Option[Int] = None,
    taskId: Option[TaskId] = None
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[Job, NoStream, Effect.Write with Effect.Read with Effect.Transactional] = {
    for {
      _ <- this += JobRecord(
        jobId,
        payloadId,
        organizationId,
        userId,
        JobState.terminalStates.contains(jobState),
        taskId
      )
      _ <- state += JobStateRecord(jobId, state = jobState)
      job <- this.get(jobId)
    } yield job.get
  }.transactionally

  @deprecated("don't use outside of tests", "2019-01-25")
  def update(
    j: JobRecord,
    s: JobStateRecord,
    payload: Payload
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[Job, NoStream, Effect.Write with Effect.Read with Effect.Transactional] = {
    for {
      _ <- PayloadsMapper.create(payload, Some(payload.packageId))
      _ <- this.insertOrUpdate(j)
      _ <- state += s
      job <- this.get(j.id).map(_.get)
    } yield job
  }.transactionally

  def updateState[T <: JobState with JobState.NoTaskId](
    id: JobId,
    newState: T,
    sentAt: OffsetDateTime = OffsetDateTime.now(UTC)
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[Int, NoStream, Effect.Write with Effect.Transactional] =
    for {
      _ <- if (JobState.terminalStates.contains(newState))
        this.filter(_.id === id).map(_.completed).update(true)
      else DBIO.successful(())
      updated <- (state += JobStateRecord(id, sentAt = sentAt, state = newState)).transactionally
    } yield updated

  def updateState(
    id: JobId,
    newState: JobState,
    taskId: TaskId,
    sentAt: OffsetDateTime
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[Unit, NoStream, Effect.Write with Effect.Transactional] = {
    for {
      _ <- if (JobState.terminalStates.contains(newState))
        this
          .filter(_.id === id)
          .map(job => (job.completed, job.taskId))
          .update((true, Some(taskId)))
      else this.filter(_.id === id).map(_.taskId).update(Some(taskId))
      _ <- state += JobStateRecord(id, sentAt = sentAt, state = newState)
    } yield ()
  }.transactionally

  def createWithPayload(
    jobId: JobId,
    payload: Payload,
    organizationId: OrganizationId,
    state: JobState,
    userId: Option[UserId] = None,
    taskId: Option[TaskId] = None
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[Job, NoStream, Effect.Write with Effect.Read with Effect.Transactional] = {
    for {
      // insert payload into database
      payloadEntry <- PayloadsMapper.create(payload, Some(payload.packageId))

      // create an associated job in the database and return new job
      job <- this.create(
        jobId,
        payloadEntry.id,
        organizationId.value,
        jobState = state,
        userId.map(_.value),
        taskId
      )
    } yield job
  }.transactionally

  def getWithPayloadEntry(
    jobId: JobId
  ): SqlAction[Option[(Job, PayloadEntry)], NoStream, Effect.Read] =
    withCurrentState()
      .join(PayloadsMapper)
      .on { case ((jobsTable, _), payloadsTable) => jobsTable.payloadId === payloadsTable.id }
      .filter { case ((jobsTable, _), _) => jobsTable.id === jobId }
      .map { case (jobAndState, payloadEntry) => (toJob(jobAndState), payloadEntry) }
      .result
      .headOption

  def getJobs(states: Set[JobState]): Query[JobsTable, JobRecord, Seq] =
    withCurrentState().filter { case (_, s) => s.inSet(states) }.map(_._1)

  def getJobs(state: JobState): Query[JobsTable, JobRecord, Seq] =
    withCurrentState().filter { case (_, s) => s === state }.map(_._1)

  def getJobs(organizationId: Int, state: JobState): Query[JobsTable, JobRecord, Seq] =
    withCurrentState()
      .filter { case (j, s) => j.organizationId === organizationId && s === state }
      .map(_._1)

  def getAllJobs(
    organizationId: OrganizationId
  ): FixedSqlStreamingAction[Seq[Job], Job, Effect.Read] =
    withCurrentState()
      .filter {
        case (job, _) => job.organizationId === organizationId.value
      }
      .map(toJob)
      .result

  def getRunningJobCount(organizationId: Int): Rep[Int] =
    getJobs(organizationId, JobState.Running).size

  def hasAvailableJobs(org: Int): Rep[Boolean] =
    getJobs(org, JobState.Available).exists

  /** Get the next job fairly. The next job will be selected based on
    * the following fairness algorithm:
    *
    * 1) Given a set of organizations that have "AVAILABLE" jobs and
    *    have not exceeded their quota, find the organization that has
    *    the earliest "last submitted" date. The "last submitted" date
    *    for any given organization is the last time that a job was
    *    transitioned into the "SUBMITTED" state. If multiple
    *    organizations have the same last_submitted date, choose the
    *    organization who has used less of their quota.
    *
    * 2) Given the organization from #1 and a set of users from that
    *    organization who have "AVAILABLE" jobs, find the user within
    *    that set that has the earliest "last submitted" date.
    *
    * 3) Given the user from #2, select the user's "AVAILABLE" job
    *    that became available first
    */
  def getNextJob(
  )(implicit
    ec: ExecutionContext
  ): DBIOAction[Option[Job], NoStream, Effect.Read with Effect.Read with Effect.Write with Effect.Transactional] = {
    val inMotionStates = JobState.inMotionStates.map(_.entryName).mkString("('", "','", "')")
    val nextJob = sql"""
    SELECT
        id,
        payload_id,
        organization_id,
        user_id,
        current_state,
        completed,
        task_id,
        last_submitted,
        created_at,
        updated_at
    FROM (
        /*
         * This subquery joins the "current state" and "last submitted" subqueries below, and
         * computes the number of in-motion jobs for each organization and the number of jobs
         * currently using a specific package for each org.
         */
        SELECT jobs_with_aggregated_attributes.*,
            SUM(
                CASE WHEN jobs_with_aggregated_attributes.current_state IN #$inMotionStates
                THEN 1 ELSE 0 END
            ) OVER (
                PARTITION BY jobs_with_aggregated_attributes.organization_id
            ) AS organization_in_motion_jobs,
            SUM(
                CASE WHEN jobs_with_aggregated_attributes.current_state IN #$inMotionStates
                THEN 1 ELSE 0 END
            ) OVER (
                PARTITION BY jobs_with_aggregated_attributes.package_id, jobs_with_aggregated_attributes.organization_id
            ) AS package_in_motion_jobs,
            oq.slots_allowed,
            org_user_last_submitted.last_submitted
        FROM (
            /*
             * This subquery gets all incomplete jobs, and computes the current state for each one.
             */
            SELECT DISTINCT ON (j.id) j.*,
                js.sent_at,
                js.state AS current_state,
                p.package_id
            FROM etl.jobs j
                INNER JOIN etl.job_state js ON j.id = js.job_id AND j.completed = false
                INNER JOIN etl.payloads p ON j.payload_id=p.id
            WHERE NOT j.completed
            ORDER BY j.id, js.sent_at DESC
        ) jobs_with_aggregated_attributes
        INNER JOIN (
            /*
             * This subquery gets the last_submitted date for each organization and user.
             */
            SELECT organization_id, user_id,
                MAX(COALESCE(submitted_at, '1900-01-01')) AS last_submitted
            FROM etl.jobs
            GROUP BY organization_id, user_id
        ) org_user_last_submitted USING (organization_id, user_id)
        INNER JOIN etl.organization_quota oq USING (organization_id)
    ) jobs_with_in_motion_count

    /*
     * Limit the result set only to where the amount of in-motion jobs for each organization
     * has not exceeded their quota, and the package touched by this job is not being worked on
     * and only return jobs that have a state of 'AVAILABLE'.
     */
    WHERE organization_in_motion_jobs < slots_allowed AND current_state = ${JobState.Available.entryName}
    AND package_in_motion_jobs = 0

    /*
     * Organizations/users that have not had a submitted job in the longest time will go first.
     * Additional ties will be broken by choosing the organization that has used less of its quota.
     * Further ties will be broken by selecting the oldest job.
     */
    ORDER BY last_submitted, organization_in_motion_jobs::float/slots_allowed::float, sent_at
    LIMIT 1"""
      .as[Job]
      .headOption

    for {
      job <- nextJob
      _ <- DBIO.sequenceOption(job.map(j => state += JobStateRecord(j.id, JobState.Submitted)))
    } yield job.map(_.copy(state = JobState.Submitted))
  }.transactionally.withTransactionIsolation(TransactionIsolation.Serializable)

  def getActiveJobsStartedBefore(
    time: OffsetDateTime
  )(implicit
    ec: ExecutionContext
  ): DBIOAction[Seq[Job], NoStream, Effect.Read] =
    this
      .getJobs(JobState.Running)
      .filter(_.submittedAt.map(_ <= time))
      .result
      .map { jobRecords =>
        jobRecords.map { jobRecord =>
          combineJobStateRecord((jobRecord, JobStateRecord(jobRecord.id, state = JobState.Running)))
        }
      }

  def getJobsByStateAndTimeFilteredByRetries(
    time: OffsetDateTime,
    state: JobState,
    maxRetries: Int
  )(implicit
    ec: ExecutionContext
  ): DBIOAction[Seq[Job], NoStream, Effect.Read] =
    withAllStates()
      .join(JobStateMapper.currentState)
      .on {
        case ((job, _), (jobId, _)) => job.id === jobId
      }
      .map {
        case ((job, jobState), (_, currentState)) => (job, jobState, currentState)
      }
      .filter {
        case (_, jobState, currentState) =>
          jobState.sentAt <= time && jobState.state === state && currentState === jobState.state
      }
      .groupBy {
        case (job, jobState, _) => (job, jobState.state)
      }
      .map {
        case ((job, jobState), group) => ((job, jobState), group.length)
      }
      .filter {
        case (_, count) => count <= maxRetries
      }
      .map {
        case (jobWithState, _) => jobWithState
      }
      .result
      .map {
        _.map {
          case (job, jobState) =>
            combineJobStateRecord(job, JobStateRecord(job.id, state = jobState))
        }
      }

  def getStateRetries(jobId: JobId, state: JobState): DBIOAction[Int, NoStream, Effect.Read] =
    withAllStates()
      .filter {
        case (job, jobState) => job.id === jobId && jobState.state === state
      }
      .length
      .result

  private def toOffsetDateTime(t: Timestamp): OffsetDateTime = t.toInstant.atOffset(UTC)

  implicit private val getJobResult: GetResult[Job] =
    GetResult { r =>
      Job(
        id = JobId(UUID.fromString(r.<<[String])),
        payloadId = r.<<,
        organizationId = r.<<,
        userId = r.<<?[Int],
        state = JobState.withName(r.<<[String]),
        completed = r.<<,
        taskId = r.<<?[Json].flatMap(_.as[TaskId].toOption),
        submittedAt = r.<<?[Timestamp].map(t => toOffsetDateTime(t)),
        createdAt = toOffsetDateTime(r.<<[Timestamp]),
        updatedAt = toOffsetDateTime(r.<<[Timestamp])
      )
    }

  /**
    *
    * Returns a page + 1 the page size specified to enable the consumer of the result
    * of this function to determine whether pagination is necessary.
    *
    * @param packageId The packageId to get jobs for
    * @param pageSize The size of the page to be selected
    * @param startAtCursor The cursor to use to determine starting location
    * @param ec The execution context to execute the futures in
    * @return sequence of jobs that represents a page
    */
  def getJobsByPackageId(
    packageId: Int,
    pageSize: Int,
    startAtCursor: Option[Cursor]
  ): DBIOAction[Seq[Job], NoStream, Effect.Read] = {
    val cursorFilter =
      startAtCursor
        .fold("") { cursor =>
          s"AND j.created_at >= '${cursor.createdAt}' AND j.id >= '${cursor.jobId}'"
        }

    sql"""
          SELECT
            j.id,
            j.payload_id,
            j.organization_id,
            j.user_id,
            LAST_VALUE(js.state) OVER(
                                 PARTITION BY js.job_id ORDER BY js.sent_at
                                 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                                 ) AS current_state,
            j.completed,
            j.task_id,
            j.submitted_at,
            j.created_at,
            j.updated_at
            FROM etl.jobs j INNER JOIN etl.job_state js ON j.id = js.job_id
                            INNER JOIN etl.payloads p ON j.payload_id = p.id
         WHERE p.package_id IS NOT NULL AND p.package_id = #$packageId #$cursorFilter
         ORDER BY j.created_at, j.id
         LIMIT #${pageSize + 1};
     """.as[Job]
  }

  def getLastJobNotAppend(
    datasetId: Int,
    packageId: Int,
    organizationId: Int
  ): DBIOAction[Option[Job], NoStream, Effect.Read] =
    //to determine the state of a package based on its jobs, we need to remove the append jobs since they do no longer
    //change the state of a package. Therefore, they are unnecessary to compute the state of the parent package.
    //Append jobs are the only ones to have a "channels" key in the payload, we use that to identify them
    sql"""
          SELECT
            j.id,
            j.payload_id,
            j.organization_id,
            j.user_id,
            LAST_VALUE(js.state) OVER(
                                 PARTITION BY js.job_id ORDER BY js.sent_at
                                 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                                 ) AS current_state,
            j.completed,
            j.task_id,
            j.submitted_at,
            j.created_at,
            j.updated_at,
            js.created_at as job_created_at
            FROM etl.jobs j INNER JOIN etl.job_state js ON j.id = js.job_id
                            INNER JOIN etl.payloads p ON j.payload_id = p.id
         WHERE p.package_id IS NOT NULL AND CAST(p.payload#>>'{datasetId}' AS int) = $datasetId AND p.package_id = $packageId AND j.organization_id = $organizationId
         AND p.payload#>>'{channels}' IS NULL
         ORDER BY job_created_at DESC limit 1;
     """
      .as[Job]
      .headOption

}
