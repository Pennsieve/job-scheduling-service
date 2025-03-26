// Copyright (c) [2018] - [2025] Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.jobscheduling.clients
import cats.data.EitherT
import cats.implicits._
import com.pennsieve.jobscheduling.model.EventualResult.EventualResultT

import scala.concurrent.{ ExecutionContext, Future }

object Notifications {

  def noOp(
  )(implicit
    ec: ExecutionContext
  ): EventualResultT[Unit] = {
    EitherT.pure[Future, Throwable](())
  }
}
