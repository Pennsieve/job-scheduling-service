// Copyright (c) [2018] - [2022] Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.jobscheduling.watchdog
import java.time.OffsetDateTime

import software.amazon.awssdk.services.ecs.model.{
  DescribeTasksRequest,
  DescribeTasksResponse,
  StopTaskRequest,
  StopTaskResponse
}
import com.pennsieve.jobscheduling.JobSchedulingPorts._
import com.pennsieve.jobscheduling.clients.{ ManifestS3Client, PennsieveApiClient }
import com.pennsieve.jobscheduling.db.Job
import com.pennsieve.jobscheduling.db.JobsMapper.getActiveJobsStartedBefore
import com.pennsieve.jobscheduling.db.profile.api.Database
import com.pennsieve.jobscheduling.model.EventualResult.EventualResult
import com.pennsieve.jobscheduling.scheduler.JobScheduler
import com.pennsieve.jobscheduling.watchdog.WatchDogPorts.{
  DescribeTasks,
  GetOldActiveJobs,
  StopTask
}
import com.pennsieve.service.utilities.ContextLogger

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
  type StopTask = StopTaskRequest => EventualResult[StopTaskResponse]
  type DescribeTasks = DescribeTasksRequest => EventualResult[DescribeTasksResponse]
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
