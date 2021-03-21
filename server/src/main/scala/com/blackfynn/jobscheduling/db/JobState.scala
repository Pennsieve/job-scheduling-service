// Copyright (c) [2018] - [2021] Pennsieve, Inc. All Rights Reserved.

package com.blackfynn.jobscheduling.db

import java.time.OffsetDateTime
import java.time.ZoneOffset.UTC
import java.util.UUID

import com.blackfynn.jobscheduling.db.profile.api._
import com.blackfynn.jobscheduling.commons.JobState
import com.blackfynn.jobscheduling.commons.JobState._
import com.blackfynn.models.PackageState._
import com.blackfynn.models.{ JobId, PackageState }
import com.github.tminglei.slickpg.window.PgWindowFuncSupport.WindowFunctions

object JobStateHelpers {

  def isSuccess(state: JobState): Boolean =
    state match {
      case Succeeded | NotProcessing => true
      case _ => false
    }

  def shouldSendNotification(state: JobState): Boolean =
    state match {
      case Succeeded | NotProcessing | Failed => true
      case _ => false
    }

  def toPackageState(jobState: JobState): PackageState = jobState match {
    case Uploading => PackageState.UNAVAILABLE
    case NotProcessing => PackageState.UPLOADED
    case Available => PackageState.PROCESSING
    case Submitted => PackageState.PROCESSING
    case Sent => PackageState.PROCESSING
    case Pending => PackageState.PROCESSING
    case Running => PackageState.PROCESSING
    case Succeeded => PackageState.READY
    case Cancelled => PackageState.DELETING
    case Failed => PackageState.PROCESSING_FAILED
    case Lost => PackageState.PROCESSING_FAILED
  }
}

/*
 * A record in the `job_state` table
 */
final case class JobStateRecord(
  jobId: JobId,
  state: JobState,
  id: UUID = UUID.randomUUID(),
  sentAt: OffsetDateTime = OffsetDateTime.now(UTC),
  createdAt: OffsetDateTime = OffsetDateTime.now(UTC)
)

/*
 * This is a transactional table containing all states attained any job
 */
final class JobStateTable(tag: Tag) extends Table[JobStateRecord](tag, Some(schema), "job_state") {
  def id = column[UUID]("id", O.PrimaryKey)
  def jobId = column[JobId]("job_id")
  def state = column[JobState]("state")
  def sentAt = column[OffsetDateTime]("sent_at")
  def createdAt = column[OffsetDateTime]("created_at")

  def job =
    foreignKey("id", jobId, TableQuery[JobsTable])(
      _.id,
      onUpdate = ForeignKeyAction.Restrict,
      onDelete = ForeignKeyAction.Cascade
    )

  def * =
    (jobId, state, id, sentAt, createdAt).mapTo[JobStateRecord]
}

object JobStateMapper extends TableQuery(new JobStateTable(_)) {
  type CurrentState = Query[(Rep[JobId], Rep[JobState]), (JobId, JobState), Seq]

  /*
   * Get the current state for each jobId by choosing the state with
   * the latest sentAt date for each jobId. If a job has two state
   * records with equal sentAt dates, ties will be broken by choosing
   * the state with the highest weight given by getWeight
   */
  def currentState: CurrentState =
    this
      .map(
        stateRecord =>
          (
            stateRecord.jobId,
            WindowFunctions.lastValue(stateRecord.state.?) :: Over
              .partitionBy(stateRecord.jobId)
              .sortBy(stateRecord.sentAt)
              .rowsFrame(RowCursor.UnboundPreceding, Some(RowCursor.UnboundFollowing))
          )
      )
      .distinct
}
