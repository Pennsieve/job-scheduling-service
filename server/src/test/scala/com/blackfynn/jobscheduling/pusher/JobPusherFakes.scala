// Copyright (c) [2018] - [2020] Blackfynn, Inc. All Rights Reserved.

package com.blackfynn.jobscheduling.pusher

import com.blackfynn.jobscheduling.JobSchedulingPorts.GetPayload
import com.blackfynn.jobscheduling.TestConfig.staticEcsConfig
import com.blackfynn.jobscheduling.TestPayload.uploadPayload
import com.blackfynn.jobscheduling.TestTask.runTaskResult
import com.blackfynn.jobscheduling.db.PayloadEntry
import com.blackfynn.jobscheduling.pusher.JobPusherPorts.{ PutManifest, RunTask }
import com.blackfynn.jobscheduling.{ ECSConfig, PusherConfig }
import com.blackfynn.models.{ ETLWorkflow, FileType, PackageType }

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
