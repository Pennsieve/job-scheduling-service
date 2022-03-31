// Copyright (c) [2018] - [2022] Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.jobscheduling.db

import java.util.UUID

import com.pennsieve.jobscheduling.commons.JobState
import com.pennsieve.models.{ JobId, Payload }
import com.github.tminglei.slickpg.{ ExPostgresProfile, PgCirceJsonSupport, PgDate2Support }
import io.circe.Json
import io.circe.syntax._

trait PostgresProfile extends ExPostgresProfile with PgDate2Support with PgCirceJsonSupport {

  override val pgjson = "jsonb" // jsonb support is in postgres 9.4.0 onward; for 9.3.x use "json"

  object PostgresAPI
      extends API
      with CirceImplicits
      with CirceJsonPlainImplicits
      with DateTimeImplicits
      with Date2DateTimePlainImplicits {

    def assert[T](predicate: => Boolean)(error: => Error): DBIO[Unit] =
      if (predicate) DBIO.successful(())
      else DBIO.failed(error)

    implicit val jobStateMapper =
      MappedColumnType
        .base[JobState, String](_.entryName, JobState.withName)

    implicit val taskIdMapper =
      MappedColumnType
        .base[Option[TaskId], Json](_.asJson, json => json.as[Option[TaskId]].right.get)

    implicit val jobIdMapper =
      MappedColumnType
        .base[JobId, UUID](_.value, uuid => JobId(uuid))

    // payload mapping

    implicit val payloadMapper =
      MappedColumnType
        .base[Payload, Json](_.asJson, _.as[Payload].right.get)
  }

  override val api = PostgresAPI
}
