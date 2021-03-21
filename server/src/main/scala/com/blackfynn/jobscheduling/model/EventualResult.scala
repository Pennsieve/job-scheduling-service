// Copyright (c) [2018] - [2021] Pennsieve, Inc. All Rights Reserved.

package com.blackfynn.jobscheduling.model
import akka.http.scaladsl.model.StatusCode
import cats.data.EitherT
import cats.implicits._
import com.blackfynn.jobscheduling.ETLLogContext

import scala.concurrent.{ ExecutionContext, Future }

object EventualResult {
  type EventualResult[A] = Future[Either[Throwable, A]]
  type EventualResultT[A] = EitherT[Future, Throwable, A]
  type EventualResponseT[A] = EitherT[Future, (StatusCode, Throwable), A]
  type EventualResultContext[A] = Future[Either[(Throwable, ETLLogContext), A]]
  type EventualResultContextT[A] = EitherT[Future, (Throwable, ETLLogContext), A]
  type EitherContext[A] = Either[(Throwable, ETLLogContext), A]

  implicit class RichFuture[A](fut: Future[A]) {
    def toEventualResultT(implicit ec: ExecutionContext): EventualResultT[A] =
      EitherT {
        fut
          .map(_.asRight[Throwable])
          .recover {
            case e => e.asLeft[A]
          }
      }
  }
}
