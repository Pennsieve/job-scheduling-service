// Copyright (c) [2018] - [2022] Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.jobscheduling.pusher

import com.pennsieve.jobscheduling.JobSchedulingPorts.GetPayload
import com.pennsieve.jobscheduling.TestConfig.staticEcsConfig
import com.pennsieve.jobscheduling.TestPayload.uploadPayload
import com.pennsieve.jobscheduling.TestTask.runTaskResult
import com.pennsieve.jobscheduling.db.PayloadEntry
import com.pennsieve.jobscheduling.pusher.JobPusherPorts.{ PutManifest, RunTask }
import com.pennsieve.jobscheduling.{ ECSConfig, PusherConfig }
import com.pennsieve.models.{ ETLWorkflow, FileType, PackageType }

import scala.concurrent.Future

object JobPusherFakes {
  val defaultPusherConfig = PusherConfig(environment = "local")

  val defaultEcsConfig: ECSConfig = staticEcsConfig

  def pusherPorts(
    runTask: RunTask = successfulRunTask,
    putManifest: PutManifest = stubManifestPut,
    getPayload: GetPayload = stubManifestGet
  ): JobPusherPorts =
    JobPusherPorts(runTask, putManifest, getPayload)

  val stubManifestPut: PutManifest =
    (_, _) => Future.successful[Either[Throwable, Unit]](Right(Unit))

  val workflowPayload: ETLWorkflow =
    ETLWorkflow(
      packageId = 1,
      datasetId = 1,
      userId = 1,
      fileType = FileType.AFNI,
      packageType = PackageType.CSV,
      files = List.empty[String],
      assetDirectory = "storage",
      encryptionKey = "key"
    )

  val stubManifestGet: GetPayload =
    _ => Future.successful(Some(PayloadEntry(workflowPayload)))

  val successfulRunTask: RunTask =
    _ => Future.successful(Right(runTaskResult()))
}
