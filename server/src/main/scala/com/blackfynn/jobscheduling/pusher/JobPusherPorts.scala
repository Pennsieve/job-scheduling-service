// Copyright (c) [2018] - [2021] Pennsieve, Inc. All Rights Reserved.

package com.blackfynn.jobscheduling.pusher

import com.amazonaws.services.ecs.model.{
  ListTasksRequest,
  ListTasksResult,
  RunTaskRequest,
  RunTaskResult
}
import com.blackfynn.jobscheduling.JobSchedulingPorts.{ createGetPayload, GetPayload }
import com.blackfynn.jobscheduling.clients.{ ECSClient, ManifestS3, ManifestS3Client }
import com.blackfynn.jobscheduling.db.profile.api._
import com.blackfynn.jobscheduling.model.EventualResult.EventualResult
import com.blackfynn.jobscheduling.pusher.JobPusherPorts.{ ListTasks, PutManifest, RunTask }

import scala.concurrent.{ ExecutionContext => EC }

case class JobPusherPorts(runTask: RunTask, putManifest: PutManifest, getPayload: GetPayload)

object JobPusherPorts {
  type PutManifest =
    (ManifestS3, EC) => EventualResult[Unit]

  type ListTasks =
    ListTasksRequest => EventualResult[ListTasksResult]

  type RunTask = RunTaskRequest => EventualResult[RunTaskResult]

  def apply(ecsClient: ECSClient, s3Client: ManifestS3Client, db: Database): JobPusherPorts =
    JobPusherPorts(
      runTask = ecsClient.runTask,
      putManifest = s3Client.putManifest,
      getPayload = createGetPayload(db)
    )
}
