// Copyright (c) [2018] - [2021] Pennsieve, Inc. All Rights Reserved.

package com.blackfynn.jobscheduling

import com.blackfynn.jobscheduling.model.ETLEvent
import com.blackfynn.models.JobId
import com.blackfynn.service.utilities.LogContext

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
