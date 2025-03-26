// Copyright (c) [2018] - [2025] Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.jobscheduling

import com.pennsieve.jobscheduling.model.ETLEvent
import com.pennsieve.models.JobId
import com.pennsieve.service.utilities.LogContext

final case class ETLLogContext(
  importId: Option[JobId] = None,
  organizationId: Option[Int] = None,
  userId: Option[Int] = None,
  taskArn: Option[String] = None
) extends LogContext {
  override val values: Map[String, String] = inferValues(this)
}

object ETLLogContext {
  def apply(event: ETLEvent): ETLLogContext =
    ETLLogContext(Some(event.importId), Some(event.organizationId), event.userId)
}
