// Copyright (c) [2018] - [2025] Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.jobscheduling.model
import java.nio.charset.StandardCharsets
import java.time.{ Instant, OffsetDateTime, ZoneOffset }
import java.util.{ Base64, UUID }

import cats.implicits._
import com.pennsieve.models.JobId

import scala.util.Try

case class Cursor(jobId: JobId, createdAt: OffsetDateTime) {
  override def toString: String =
    Base64.getEncoder
      .encodeToString {
        s"job:$jobId:${createdAt.toInstant.toEpochMilli}".getBytes(StandardCharsets.UTF_8)
      }
}

object Cursor {
  type InvalidCursorException = InvalidCursorException.type
  private val IdExtractor =
    "job:([a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}):(\\d+)".r

  def fromString(s: String): Either[Throwable, Cursor] =
    Try(new String(Base64.getDecoder.decode(s.getBytes(StandardCharsets.UTF_8)))).toEither
      .flatMap { sentCursorString =>
        for {
          elements <- {
            IdExtractor
              .unapplySeq(sentCursorString)
              .toRight[Throwable](InvalidCursorException)
          }

          uuid <- {
            elements.headOption
              .toRight(InvalidCursorException)
              .flatMap(s => Try(UUID.fromString(s)).toEither)
          }

          time <- {
            elements.lastOption
              .toRight(InvalidCursorException)
              .flatMap { timeString =>
                Try(
                  OffsetDateTime.ofInstant(Instant.ofEpochMilli(timeString.toLong), ZoneOffset.UTC)
                ).toEither
              }
          }
        } yield Cursor(JobId(uuid), time)
      }
      .leftMap(_ => InvalidCursorException)
}

case object InvalidCursorException extends Exception {
  override def getMessage: String = "Cursor was a not a valid structure"
}
