// Copyright (c) [2018] - [2021] Pennsieve, Inc. All Rights Reserved.

package com.blackfynn.jobscheduling.watchdog
import java.time.OffsetDateTime

import com.amazonaws.services.ecs.model.{
  DescribeTasksRequest,
  DescribeTasksResult,
  StopTaskRequest,
  StopTaskResult
}
import com.blackfynn.jobscheduling.JobSchedulingPorts._
import com.blackfynn.jobscheduling.clients.{ ManifestS3Client, PennsieveApiClient }
import com.blackfynn.jobscheduling.db.Job
import com.blackfynn.jobscheduling.db.JobsMapper.getActiveJobsStartedBefore
import com.blackfynn.jobscheduling.db.profile.api.Database
import com.blackfynn.jobscheduling.model.EventualResult.EventualResult
import com.blackfynn.jobscheduling.scheduler.JobScheduler
import com.blackfynn.jobscheduling.watchdog.WatchDogPorts.{
  DescribeTasks,
  GetOldActiveJobs,
  StopTask
}
import com.blackfynn.service.utilities.ContextLogger

import scala.concurrent.{ ExecutionContext, Future }

case class WatchDogPorts(
  getOldActiveJobs: GetOldActiveJobs,
  stopTask: StopTask,
  describeTasks: DescribeTasks,
  getManifest: GetManifest,
  getJob: GetJob,
  getPayload: GetPayload,
  updateJob: UpdateJob,
  notifyJobSource: NotifyJobSource,
  pennsieveApiClient: PennsieveApiClient
)

object WatchDogPorts {
  type StopTask = StopTaskRequest => EventualResult[StopTaskResult]
  type DescribeTasks = DescribeTasksRequest => EventualResult[DescribeTasksResult]
  type GetOldActiveJobs = OffsetDateTime => Future[Seq[Job]]

  def createGetOldActiveJobs(db: Database)(implicit ec: ExecutionContext): GetOldActiveJobs =
    offset => db.run(getActiveJobsStartedBefore(offset))

  def apply(
    db: Database,
    stopTask: StopTask,
    describeTasks: DescribeTasks,
    pennsieveApiClient: PennsieveApiClient,
    manifestClient: ManifestS3Client,
    scheduler: JobScheduler
  )(implicit
    ec: ExecutionContext,
    log: ContextLogger
  ): WatchDogPorts =
    WatchDogPorts(
      getOldActiveJobs = createGetOldActiveJobs(db),
      stopTask = stopTask,
      describeTasks = describeTasks,
      getManifest = manifestClient.getManifest,
      getJob = createGetJob(db),
      getPayload = createGetPayload(db),
      createUpdateJob(db),
      createNotifyJobSource(scheduler),
      pennsieveApiClient = pennsieveApiClient
    )
}
