// Copyright (c) [2018] - [2021] Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.jobscheduling.db

import com.pennsieve.models.Payload
import com.pennsieve.jobscheduling.db.profile.api._
import java.time.{ OffsetDateTime, ZoneOffset }

import slick.dbio.{ Effect, NoStream }
import slick.sql.{ FixedSqlAction, SqlAction }

final case class PayloadEntry(
  payload: Payload,
  packageId: Option[Int] = None,
  createdAt: OffsetDateTime = OffsetDateTime.now(ZoneOffset.UTC),
  updatedAt: OffsetDateTime = OffsetDateTime.now(ZoneOffset.UTC),
  id: Int = 0
)

final class PayloadsTable(tag: Tag) extends Table[PayloadEntry](tag, Some(schema), "payloads") {

  def payload = column[Payload]("payload")
  def packageId = column[Option[Int]]("package_id")

  def createdAt = column[OffsetDateTime]("created_at")
  def updatedAt = column[OffsetDateTime]("updated_at")
  def id = column[Int]("id", O.PrimaryKey, O.AutoInc)

  def * = (payload, packageId, createdAt, updatedAt, id).mapTo[PayloadEntry]
}

object PayloadsMapper extends TableQuery(new PayloadsTable(_)) {

  def get(id: Int): SqlAction[Option[PayloadEntry], NoStream, Effect.Read] =
    this.filter(_.id === id).result.headOption

  def create(
    payload: Payload,
    packageId: Option[Int]
  ): FixedSqlAction[PayloadEntry, NoStream, Effect.Write] =
    (this returning this) += PayloadEntry(payload, packageId)

}
