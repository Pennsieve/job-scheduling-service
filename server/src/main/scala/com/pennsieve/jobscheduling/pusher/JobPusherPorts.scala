// Copyright (c) [2018] - [2025] Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.jobscheduling.pusher

import software.amazon.awssdk.services.ecs.model.{
  ListTasksRequest,
  ListTasksResponse,
  RunTaskRequest,
  RunTaskResponse
}
import com.pennsieve.jobscheduling.JobSchedulingPorts.{ createGetPayload, GetPayload }
import com.pennsieve.jobscheduling.clients.{ ECSClient, ManifestS3, ManifestS3Client }
import com.pennsieve.jobscheduling.db.profile.api._
import com.pennsieve.jobscheduling.model.EventualResult.EventualResult
import com.pennsieve.jobscheduling.pusher.JobPusherPorts.{ ListTasks, PutManifest, RunTask }

import scala.concurrent.{ ExecutionContext => EC }

case class JobPusherPorts(runTask: RunTask, putManifest: PutManifest, getPayload: GetPayload)

object JobPusherPorts {
  type PutManifest =
    (ManifestS3, EC) => EventualResult[Unit]

  type ListTasks =
    ListTasksRequest => EventualResult[ListTasksResponse]

  type RunTask = RunTaskRequest => EventualResult[RunTaskResponse]

  def apply(ecsClient: ECSClient, s3Client: ManifestS3Client, db: Database): JobPusherPorts =
    JobPusherPorts(
      runTask = ecsClient.runTask,
      putManifest = s3Client.putManifest,
      getPayload = createGetPayload(db)
    )
}
