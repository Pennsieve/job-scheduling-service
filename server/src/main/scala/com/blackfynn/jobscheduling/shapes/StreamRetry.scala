// Copyright (c) [2018] - [2021] Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.jobscheduling.shapes

import java.time.OffsetDateTime
import java.time.ZoneOffset.UTC
import java.time.temporal.ChronoUnit.MILLIS

import akka.actor.Scheduler
import akka.pattern.after
import com.pennsieve.jobscheduling.RetryConfig
import com.pennsieve.service.utilities.ContextLogger

import scala.concurrent.{ ExecutionContext, Future }
import scala.util.control.NonFatal

/**
  * Retry streams on NonFatal errors.
  */
object StreamRetry {
  def apply[A](
    stream: () => Future[A],
    config: RetryConfig,
    streamName: String,
    tryNum: Int = 0,
    lastRetry: OffsetDateTime = OffsetDateTime.now(UTC)
  )(implicit
    log: ContextLogger,
    ec: ExecutionContext,
    scheduler: Scheduler
  ): Future[A] = {
    stream()
      .recoverWith {
        case NonFatal(e) =>
          val failureMessage = s"$streamName failed: ${e.getMessage}"
          val millisSinceRetriesBegan = lastRetry until (OffsetDateTime.now(UTC), MILLIS)

          // if we have been retrying for longer than the retry limit,
          // restart the retry count from 1
          if (config.resetAfter.toMillis < millisSinceRetriesBegan) {
            log.noContext.warn(s"${failureMessage}\nRetrying 1/${config.maxRetries}", e)
            after(config.delay, scheduler)(apply(stream, config, streamName, 1))
          } else if (tryNum < config.maxRetries) {
            log.noContext.warn(s"${failureMessage}\nRetrying ${tryNum + 1}/${config.maxRetries}", e)
            after(config.delay, scheduler)(apply(stream, config, streamName, tryNum + 1, lastRetry))
          } else {
            log.noContext.error(s"${failureMessage}\nRetries exceeded!", e)
            Future.failed(e)
          }
      }
  }
}
