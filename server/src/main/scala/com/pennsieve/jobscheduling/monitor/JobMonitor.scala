// Copyright (c) [2018] - [2022] Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.jobscheduling
package monitor

import akka.actor.{ ActorSystem, Scheduler }
import akka.stream.alpakka.sqs.SqsSourceSettings
import akka.stream.alpakka.sqs.scaladsl.SqsSource
import akka.stream.scaladsl.{ Flow, Keep, Source }
import akka.{ Done, NotUsed }
import cats.data.EitherT
import cats.implicits._
import software.amazon.awssdk.services.sqs.model.Message
import com.pennsieve.jobscheduling.JobSchedulingPorts.FinalSink
import com.pennsieve.jobscheduling.clients.SQSClient.{ ReceiptHandle, SendAck }
import com.pennsieve.jobscheduling.clients.{ Notifications, SQSClient }
import com.pennsieve.jobscheduling.commons.JobState.Cancelled
import com.pennsieve.jobscheduling.db.DatabaseClientFlows
import com.pennsieve.jobscheduling.model.EventualResult.{ EitherContext, EventualResultContextT }
import com.pennsieve.jobscheduling.model.{ ETLEvent, PackageLostEvent }
import com.pennsieve.jobscheduling.monitor.CloudwatchMessage._
import com.pennsieve.jobscheduling.shapes.EitherPartition._
import com.pennsieve.jobscheduling.shapes.StreamRetry
import com.pennsieve.models.Payload
import com.pennsieve.service.utilities.{ ContextLogger, Tier }
import io.circe.parser.decode
import io.circe.syntax.EncoderOps

import scala.concurrent.{ ExecutionContext, Future }

/**
  * A stream to monitor job updates from cloudwatch
  */
class JobMonitor(
  sqsMessageSource: Source[Message, NotUsed],
  etlBucket: String
)(implicit
  log: ContextLogger,
  system: ActorSystem,
  scheduler: Scheduler,
  ec: ExecutionContext,
  ports: JobMonitorPorts,
  config: JobMonitorConfig
) {
  implicit val tier: Tier[JobMonitor] = Tier[JobMonitor]

  private val cloudwatchSource = sqsMessageSource
    .map { msg =>
      decode[CloudwatchMessage](msg.body())
        .map(cloudwatchMessage => ReceiptHandle(msg) -> cloudwatchMessage)
        .leftMap(error => msg -> error)
    }
    .collectRightHandleLeftWith {
      case (message, error) =>
        log.noContext.error(s"Failed to deserialize cloud watch event: $message", error)
        ports.sendAck(ReceiptHandle(message))
    }
    .map {
      case handleWithMessage @ (_, msg) =>
        log.tierContext
          .info(s"JobMonitor decoded ${msg.asJson.noSpaces}")(msg.getLogContext)
        handleWithMessage
    }
    .mapAsyncUnordered(config.parallelism) {
      case (receiptHandle, cloudWatchMessage) =>
        cloudWatchMessage
          .toEtlEvent(etlBucket, receiptHandle, ports.getJob)
          .map {
            _.leftMap((receiptHandle, _))
          }
          .recover {
            case error: Exception => Left((receiptHandle, error))
          }
    }
    .collectRightHandleLeftWith {
      case (receiptHandle, error) =>
        log.noContext.error(s"Failed to create etl event", error)
        ports.sendAck(receiptHandle)
    }
    .map { event =>
      log.tierContext.info(s"JobMonitor create event ${event.asJson}")(event.logContext)
      event
    }

  private def ackEvent(
    sendAck: SendAck
  )(
    event: ETLEvent
  )(implicit
    log: ContextLogger,
    ec: ExecutionContext
  ): EventualResultContextT[ETLEvent] =
    event.receiptHandle match {
      case Some(receiptHandle) =>
        log.tierContext.info(s"JobMonitor Acked ${event.asJson.noSpaces}")(event.logContext)
        EitherT(sendAck(receiptHandle)).map(_ => event).leftMap((_, event.logContext))
      case None =>
        log.tierContext.info("JobMonitor didn't send Ack no message to Ack")(event.logContext)
        EitherT.pure[Future, (Throwable, ETLLogContext)](event)
    }

  private def updateAndNotifyFlow(
    implicit
    ec: ExecutionContext,
    ports: JobMonitorPorts,
    config: JobMonitorConfig
  ): Flow[ETLEvent, EitherContext[ETLEvent], NotUsed] =
    Flow[ETLEvent]
      .via(
        DatabaseClientFlows
          .updateDatabaseFlow(config.throttle, ports.updateJob, ports.notifyJobSource)
      )
      .via(
        DatabaseClientFlows
          .getPayloadFlow(config.throttle, ports.getPayload, ports.getJob, ports.getManifest)
      )
      .via(ports.pennsieveApiClient.updatePackageFlow(config.throttle))
      .mapAsyncUnordered[EitherContext[(ETLEvent, Payload)]](config.throttle.parallelism) {
        case Right((event: PackageLostEvent, payload)) =>
          ports
            .updateJob(event.importId, Cancelled, event.sentAt, None, event.logContext)
            .map {
              _.map(_ => (event, payload))
            }

        case Right(eventWithPayload) => Future.successful(Right(eventWithPayload))

        case Left(errorWithContext) => Future.successful(Left(errorWithContext))
      }
      .mapAsyncUnordered(config.throttle.parallelism) {
        case Right((event, payload)) =>
          Notifications
            .sendNotification(
              "JobMonitor",
              event.importId,
              event.organizationId,
              event.jobState,
              payload,
              ports.sendMessage
            )
            .leftMap((_, event.logContext))
            .flatMap(_ => ackEvent(ports.sendAck)(event))
            .value

        case Left(errorContext) => Future.successful(Left(errorContext))
      }

  def run(finalSink: FinalSink, tryNum: Int = 0): Future[Done] =
    StreamRetry(
      () =>
        cloudwatchSource
          .via(updateAndNotifyFlow)
          .toMat(finalSink)(Keep.right)
          .run(),
      config.retry,
      "JobMonitor"
    )
}

object JobMonitor {
  def apply(
    jobMonitorConfig: JobMonitorConfig,
    jobMonitorPorts: JobMonitorPorts,
    etlBucket: String
  )(implicit
    log: ContextLogger,
    system: ActorSystem,
    scheduler: Scheduler,
    ec: ExecutionContext
  ): JobMonitor = {
    implicit val config = jobMonitorConfig
    implicit val ports = jobMonitorPorts

    val sqsSourceSettings = SqsSourceSettings.Defaults.withMaxBatchSize(config.parallelism)
    val sqsMessageSource: Source[Message, NotUsed] =
      SqsSource(config.queue, sqsSourceSettings)(ports.sqsClient)

    new JobMonitor(sqsMessageSource, etlBucket)
  }
}
