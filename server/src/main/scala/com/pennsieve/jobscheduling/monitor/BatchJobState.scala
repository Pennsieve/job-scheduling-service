// Copyright (c) [2018] - [2022] Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.jobscheduling.monitor

import enumeratum.EnumEntry.Uppercase
import enumeratum.{ CirceEnum, Enum, EnumEntry }

sealed trait BatchJobState extends EnumEntry with Uppercase

object BatchJobState extends Enum[BatchJobState] with CirceEnum[BatchJobState] {
  val values = findValues

  case object SUCCEEDED extends BatchJobState
  case object FAILED extends BatchJobState

}
