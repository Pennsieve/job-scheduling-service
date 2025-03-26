// Copyright (c) [2018] - [2025] Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.jobscheduling.clients

import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model._
import com.pennsieve.jobscheduling.clients.AwsAsyncResponseAdapter.createRequestFunction

import com.pennsieve.jobscheduling.model.EventualResult.EventualResult
import io.circe.Encoder
import io.circe.generic.extras.semiauto.deriveUnwrappedEncoder

import java.util.function.Consumer

object SQSClient {

  case class ReceiptHandle(value: String) extends AnyVal

  object ReceiptHandle {

    implicit val encoder: Encoder[ReceiptHandle] = deriveUnwrappedEncoder[ReceiptHandle]

    def apply(message: Message): ReceiptHandle = ReceiptHandle(message.receiptHandle())
  }

  case class QueueName(value: String) extends AnyVal

  type SendAck = ReceiptHandle => EventualResult[DeleteMessageResponse]

  def createSendAck(sqsClient: SqsAsyncClient, queueName: QueueName): SendAck = {
    val buildRequest: ReceiptHandle => Consumer[DeleteMessageRequest.Builder] = receiptHandle =>
      (builder: DeleteMessageRequest.Builder) =>
        builder.queueUrl(queueName.value).receiptHandle(receiptHandle.value)

    createRequestFunction(buildRequest, sqsClient.deleteMessage)
  }

  case class MessageBody(value: String) extends AnyVal

  type SendMessage = MessageBody => EventualResult[SendMessageResponse]

  def createSendMessage(sqsClient: SqsAsyncClient, queueName: QueueName): SendMessage = {
    val buildRequest: MessageBody => Consumer[SendMessageRequest.Builder] = messageBody =>
      (builder: SendMessageRequest.Builder) =>
        builder.queueUrl(queueName.value).messageBody(messageBody.value)

    createRequestFunction(buildRequest, sqsClient.sendMessage)
  }

  type GetNumberOfMessages = () => EventualResult[GetQueueAttributesResponse]
  val ApproximateNumberOfMessages = "ApproximateNumberOfMessages"

  def createGetNumberOfMessages(
    sqsClient: SqsAsyncClient,
    queueName: QueueName
  ): GetNumberOfMessages = {
    val buildRequest: Consumer[GetQueueAttributesRequest.Builder] =
      builder => builder.queueUrl(queueName.value)
    createRequestFunction(buildRequest, sqsClient.getQueueAttributes)
  }
}
