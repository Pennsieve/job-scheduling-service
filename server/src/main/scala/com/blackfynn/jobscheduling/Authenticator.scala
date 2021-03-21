// Copyright (c) [2018] - [2021] Pennsieve, Inc. All Rights Reserved.

package com.blackfynn.jobscheduling

import com.blackfynn.auth.middleware.Jwt.Role.RoleIdentifier
import com.blackfynn.auth.middleware.{ DatasetId, Jwt, OrganizationId, ServiceClaim }
import com.blackfynn.models.Role
import shapeless.syntax.inject._

import scala.concurrent.duration.FiniteDuration

object Authenticator {

  def generateServiceToken(
    jwt: Jwt.Config,
    duration: FiniteDuration,
    organizationId: Int,
    datasetId: Int
  ): Jwt.Token = {

    val serviceClaim = ServiceClaim(
      List(
        Jwt.OrganizationRole(
          OrganizationId(organizationId).inject[RoleIdentifier[OrganizationId]],
          Role.Owner
        ),
        Jwt.DatasetRole(DatasetId(datasetId).inject[RoleIdentifier[DatasetId]], Role.Owner)
      )
    )

    val claim = Jwt.generateClaim(serviceClaim, duration)
    Jwt.generateToken(claim)(jwt)
  }

}
