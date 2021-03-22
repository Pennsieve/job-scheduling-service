// Copyright (c) [2018] - [2021] Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.jobscheduling.db
import akka.NotUsed
import akka.stream.scaladsl.Flow
import cats.implicits._
import com.pennsieve.jobscheduling.JobSchedulingPorts._
import com.pennsieve.jobscheduling.commons.JobState.{ Failed, Lost }
import com.pennsieve.jobscheduling.model.EventualResult.{ EitherContext, EventualResultContext }
import com.pennsieve.jobscheduling.model._
import com.pennsieve.jobscheduling.{ ETLLogContext, JobSchedulingPorts, ThrottleConfig }
import com.pennsieve.models._
import com.pennsieve.service.utilities.{ ContextLogger, Tier }
import io.circe.syntax.EncoderOps

import scala.concurrent.{ ExecutionContext, Future }

/*
 * Static functions for creating flows that interact with the database
 */
object DatabaseClientFlows {
  def updateDatabaseFlow(
    throttleConfig: ThrottleConfig,
    updateJobState: UpdateJob,
    notifyJobSource: NotifyJobSource
  )(implicit
    ec: ExecutionContext,
    log: ContextLogger,
    tier: Tier[_]
  ): Flow[ETLEvent, EitherContext[ETLEvent], NotUsed] =
    Flow[ETLEvent]
      .throttle(throttleConfig.parallelism, throttleConfig.period)
      .mapAsyncUnordered[EitherContext[ETLEvent]](throttleConfig.parallelism) { event =>
        for {
          maybeEvent <- {
            updateJobStateFromEvent(event, updateJobState)(log, ETLLogContext(event), tier)
              .map(_.map(_ => event))
              .recover {
                case e => Left((e, event.logContext))
              }
          }
          _ <- notifyJobSource().map(_.leftMap((_, event.logContext)))
        } yield maybeEvent
      }

  def getPayloadFlow(
    throttle: ThrottleConfig,
    getPayload: GetPayload,
    getJob: GetJob,
    getManifest: GetManifest
  )(implicit
    ec: ExecutionContext
  ): Flow[EitherContext[ETLEvent], EitherContext[(ETLEvent, Payload)], NotUsed] =
    Flow[EitherContext[ETLEvent]]
      .mapAsyncUnordered(throttle.parallelism) {
        case Right(event) =>
          JobSchedulingPorts
            .getPayloadFromEvent(event, getPayload, getJob, getManifest)
            .map(_.map((event, _)).leftMap((_, event.logContext)))
        case Left(errorContext) => Future.successful(Left(errorContext))
      }

  private def updateJobStateFromEvent(
    event: ETLEvent,
    updateJobState: UpdateJob
  )(implicit
    log: ContextLogger,
    logContext: ETLLogContext,
    tier: Tier[_]
  ): EventualResultContext[Unit] =
    event match {
      case SuccessfulEvent(jobId, taskId, jobState, _, _, _, _, _, sentAt) =>
        updateJobState(jobId, jobState, sentAt, Some(taskId), logContext)

      case event @ LostTaskEvent(jobId, jobState, _, _, _, _, _, sentAt) =>
        log.tierContext.warn(event.asJson.noSpaces)
        updateJobState(jobId, jobState, sentAt, None, logContext)

      case event @ TaskCreationFailedEvent(jobId, _, _, _, _, _, sentAt) =>
        log.tierContext.warn(event.asJson.noSpaces)
        updateJobState(jobId, Failed, sentAt, None, logContext)

      case event @ FailedLostTaskEvent(jobId, _, _, _, _, sentAt) =>
        log.tierContext.warn(event.asJson.noSpaces)
        updateJobState(jobId, Lost, sentAt, None, logContext)

      // PackageLostEvent should never pass through the flow that this function
      // is used in. This will only appear after an attempt has been made to update
      // API. If API 404s this event is created. The database should have been updated
      // before a request is sent to update the package state and the 404 is found.
      case _: PackageLostEvent => ???
    }
}
