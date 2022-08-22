// Copyright (c) [2018] - [2022] Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.jobscheduling.monitor

import software.amazon.awssdk.services.sqs.SqsAsyncClient

import com.pennsieve.jobscheduling.JobSchedulingPorts.{
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
import com.pennsieve.jobscheduling.scheduler.JobScheduler
import com.pennsieve.jobscheduling.clients.{ ManifestS3Client, PennsieveApiClient, SQSClient }
import com.pennsieve.jobscheduling.clients.SQSClient.{ QueueName, SendAck }
import com.pennsieve.jobscheduling.db.profile.api._
import scala.concurrent.ExecutionContext

case class JobMonitorPorts(
  sqsClient: SqsAsyncClient,
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
    sqsClient: SqsAsyncClient,
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
