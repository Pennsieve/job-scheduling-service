// Copyright (c) [2018] - [2025] Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.jobscheduling.handlers

import akka.http.scaladsl.model.headers.{ Authorization, OAuth2BearerToken }
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import com.pennsieve.auth.middleware.Jwt.Role.RoleIdentifier
import com.pennsieve.auth.middleware.{
  ClaimType,
  Jwt,
  OrganizationId,
  ServiceClaim,
  UserClaim,
  UserId,
  Wildcard
}
import com.pennsieve.jobscheduling.JobSchedulingServiceSpecHarness
import com.pennsieve.jobscheduling.TestPayload.{ organizationId, userId }
import com.pennsieve.jobscheduling.clients.generated.definitions.Quota
import com.pennsieve.jobscheduling.clients.generated.organizations.OrganizationsClient
import com.pennsieve.jobscheduling.db.OrganizationQuotaMapper
import com.pennsieve.jobscheduling.db.OrganizationQuotaMapper.getOrganization
import com.pennsieve.jobscheduling.db.profile.api._
import com.pennsieve.models.Role
import com.pennsieve.test.AwaitableImplicits
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import shapeless.Coproduct
import shapeless.syntax.inject.InjectSyntax

import scala.concurrent.duration.{ FiniteDuration, MINUTES }

class OrganizationHandlerSpec
    extends AnyWordSpec
    with ScalatestRouteTest
    with JobSchedulingServiceSpecHarness
    with AwaitableImplicits
    with Matchers
    with BeforeAndAfterEach {

  "POST /organizations/:organizationId/set/quota" should {
    "add an organization and it's quota to the database" in {
      val claim: Jwt.Claim = generateClaim(wildcardClaim)
      val token: Jwt.Token = Jwt.generateToken(claim)(ports.jwt)

      val client = createClient(createRoutes)

      client
        .setQuota(
          organizationId,
          Quota(tenSlotsAllowed),
          List(Authorization(OAuth2BearerToken(token.value)))
        )
        .awaitFinite()

      val actualOrganizationQuota = ports.db.run(getOrganization(organizationId)).awaitFinite().get

      actualOrganizationQuota.slotsAllowed shouldBe tenSlotsAllowed
    }

    "update an existing organizations quota" in {
      val twentySlotsAllowed = 20

      ports.db
        .run(OrganizationQuotaMapper.create(organizationId, tenSlotsAllowed))
        .awaitFinite()

      val claim: Jwt.Claim = generateClaim(wildcardClaim)
      val token: Jwt.Token = Jwt.generateToken(claim)(ports.jwt)

      val client = createClient(createRoutes)

      client
        .setQuota(
          organizationId,
          Quota(twentySlotsAllowed),
          List(Authorization(OAuth2BearerToken(token.value)))
        )
        .awaitFinite()

      val actualOrganizationQuota = ports.db.run(getOrganization(organizationId)).awaitFinite().get

      actualOrganizationQuota.slotsAllowed shouldBe twentySlotsAllowed
    }

    "reject a user who is not a super admin" in {
      val claim: Jwt.Claim = generateClaim(userClaim)
      val token: Jwt.Token = Jwt.generateToken(claim)(ports.jwt)

      val client = createClient(createRoutes)

      client
        .setQuota(
          organizationId,
          Quota(tenSlotsAllowed),
          List(Authorization(OAuth2BearerToken(token.value)))
        )
        .awaitFinite()

      val actualOrganizationQuota = ports.db.run(getOrganization(organizationId)).awaitFinite()

      actualOrganizationQuota shouldBe None
    }
  }

  val tenSlotsAllowed = 10

  override def beforeEach(): Unit = ports.db.run(OrganizationQuotaMapper.delete).awaitFinite()

  // http://doc.akka.io/docs/akka-http/10.0.0/scala/http/routing-dsl/testkit.html#testing-sealed-routes
  def createRoutes: Route = Route.seal(OrganizationsHandler.routes)

  def createClient(routes: Route) = OrganizationsClient.httpClient(Route.toFunction(routes))

  val userClaim = UserClaim(
    id = UserId(userId),
    roles = List(
      Jwt.OrganizationRole(
        id = OrganizationId(organizationId).inject[RoleIdentifier[OrganizationId]],
        role = Role.Manager
      )
    )
  )

  val wildcardClaim = ServiceClaim(
    List(Jwt.OrganizationRole(Coproduct[RoleIdentifier[OrganizationId]](Wildcard), Role.Owner))
  )

  def generateClaim(
    claimType: ClaimType,
    duration: FiniteDuration = FiniteDuration(1, MINUTES)
  ): Jwt.Claim =
    Jwt.generateClaim(claimType, duration)
}
