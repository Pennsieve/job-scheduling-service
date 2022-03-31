// Copyright (c) [2018] - [2022] Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.jobscheduling.clients

import com.amazonaws.services.sqs.AmazonSQSAsync
import com.amazonaws.services.sqs.model._
import com.pennsieve.jobscheduling.clients.AwsAsyncCallback.async
import com.pennsieve.jobscheduling.model.EventualResult.EventualResult
import io.circe.Encoder
import io.circe.generic.extras.semiauto.deriveUnwrappedEncoder

object SQSClient {

  case class ReceiptHandle(value: String) extends AnyVal

  object ReceiptHandle {

    implicit val encoder: Encoder[ReceiptHandle] = deriveUnwrappedEncoder[ReceiptHandle]

    def apply(message: Message): ReceiptHandle = ReceiptHandle(message.getReceiptHandle)
  }

  case class QueueName(value: String) extends AnyVal

  type SendAck = ReceiptHandle => EventualResult[DeleteMessageResult]

  def createSendAck(sqsClient: AmazonSQSAsync, queueName: QueueName): SendAck = { receiptHandle =>
    val asyncCallback =
      new AwsAsyncCallback[DeleteMessageRequest, DeleteMessageResult]
    sqsClient.deleteMessageAsync(queueName.value, receiptHandle.value, asyncCallback)
    asyncCallback.promise.future
  }

  case class MessageBody(value: String) extends AnyVal

  type SendMessage = MessageBody => EventualResult[SendMessageResult]

  def createSendMessage(sqsClient: AmazonSQSAsync, queueName: QueueName): SendMessage = { message =>
    val callback = new AwsAsyncCallback[SendMessageRequest, SendMessageResult]
    sqsClient.sendMessageAsync(queueName.value, message.value, callback)
    callback.promise.future
  }

  import scala.collection.JavaConverters._
  type GetNumberOfMessages = () => EventualResult[GetQueueAttributesResult]
  val ApproximateNumberOfMessages = "ApproximateNumberOfMessages"

  def createGetNumberOfMessages(
    sqsClient: AmazonSQSAsync,
    queueName: QueueName
  ): GetNumberOfMessages =
    () =>
      async(sqsClient.getQueueAttributesAsync)(
        new GetQueueAttributesRequest(queueName.value, List().asJava)
      )
}
