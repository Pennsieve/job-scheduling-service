// Copyright (c) [2018] - [2022] Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.jobscheduling.db

import java.time.OffsetDateTime

import com.pennsieve.jobscheduling.db.profile.api._
import slick.dbio.{ Effect, NoStream }
import slick.sql.SqlAction

final case class OrganizationQuota(
  organizationId: Int,
  slotsAllowed: Int,
  submittedAt: Option[OffsetDateTime] = None
)

final class OrganizationQuotaTable(tag: Tag)
    extends Table[OrganizationQuota](tag, Some(schema), "organization_quota") {

  def organizationId = column[Int]("organization_id")
  def slotsAllowed = column[Int]("slots_allowed")
  def submittedAt = column[Option[OffsetDateTime]]("submitted_at")

  def * =
    (organizationId, slotsAllowed, submittedAt)
      .mapTo[OrganizationQuota]
}

object OrganizationQuotaMapper extends TableQuery(new OrganizationQuotaTable(_)) {
  def getOrganization(
    organizationId: Int
  ): SqlAction[Option[OrganizationQuota], NoStream, Effect.Read] =
    this.filter(_.organizationId === organizationId).result.headOption

  def create(organizationId: Int, slots: Int) =
    (this returning this) += OrganizationQuota(organizationId, slots)

  def updateQuota(organizationId: Int, slotsAllowed: Int) =
    this
      .filter(_.organizationId === organizationId)
      .map(_.slotsAllowed)
      .update(slotsAllowed)
}
