// Copyright (c) [2018] - [2021] Pennsieve, Inc. All Rights Reserved.

package com.blackfynn.jobscheduling.monitor

import com.amazonaws.services.sqs.AmazonSQSAsync
import com.blackfynn.jobscheduling.JobSchedulingPorts.{
  createGetJob,
  createGetPayload,
  createNotifyJobSource,
  createUpdateJob,
  GetJob,
  GetManifest,
  GetPayload,
  NotifyJobSource,
  UpdateJob
}
import com.blackfynn.jobscheduling.scheduler.JobScheduler
import com.blackfynn.jobscheduling.clients.{ ManifestS3Client, PennsieveApiClient, SQSClient }
import com.blackfynn.jobscheduling.clients.SQSClient.{ QueueName, SendAck }
import com.blackfynn.jobscheduling.db.profile.api._
import scala.concurrent.ExecutionContext

case class JobMonitorPorts(
  sqsClient: AmazonSQSAsync,
  sendMessage: SQSClient.SendMessage,
  getManifest: GetManifest,
  getJob: GetJob,
  getPayload: GetPayload,
  updateJob: UpdateJob,
  notifyJobSource: NotifyJobSource,
  sendAck: SendAck,
  pennsieveApiClient: PennsieveApiClient
)

object JobMonitorPorts {
  def apply(
    sqsClient: AmazonSQSAsync,
    notificationsQueue: String,
    pennsieveApiClient: PennsieveApiClient,
    db: Database,
    manifestClient: ManifestS3Client,
    scheduler: JobScheduler,
    queueName: QueueName
  )(implicit
    ec: ExecutionContext
  ): JobMonitorPorts = {
    val sendAck: SendAck = SQSClient.createSendAck(sqsClient, queueName)

    JobMonitorPorts(
      sqsClient,
      SQSClient.createSendMessage(sqsClient, SQSClient.QueueName(notificationsQueue)),
      manifestClient.getManifest,
      createGetJob(db),
      createGetPayload(db),
      createUpdateJob(db),
      createNotifyJobSource(scheduler),
      sendAck,
      pennsieveApiClient
    )
  }
}
