// Copyright (c) [2018] - [2021] Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.jobscheduling

package object db {
  final val schema: String = "etl"

  object profile extends PostgresProfile
}
