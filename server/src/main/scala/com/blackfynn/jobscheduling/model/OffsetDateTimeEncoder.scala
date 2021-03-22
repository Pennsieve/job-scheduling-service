// Copyright (c) [2018] - [2021] Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.jobscheduling.model
import java.time.OffsetDateTime

import io.circe.Encoder
import io.circe.syntax.EncoderOps

object OffsetDateTimeEncoder {
  implicit val encode: Encoder[OffsetDateTime] =
    Encoder.instance(date => date.toString.asJson)
}
