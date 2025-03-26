// Copyright (c) [2018] - [2025] Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.jobscheduling.handlers

import akka.actor.ActorSystem
import akka.http.scaladsl.server.Route
import com.pennsieve.auth.middleware.AkkaDirective.authenticateJwt
import com.pennsieve.auth.middleware.Validator.{ hasOrganizationAccess, isServiceClaim }
import com.pennsieve.auth.middleware.{ Jwt, OrganizationId }
import com.pennsieve.jobscheduling.db.OrganizationQuotaMapper._
import com.pennsieve.jobscheduling.server.generated.definitions.Quota
import com.pennsieve.jobscheduling.server.generated.organizations.OrganizationsResource._
import com.pennsieve.jobscheduling.server.generated.organizations.{
  OrganizationsResource,
  OrganizationsHandler => GuardrailHandler
}
import com.pennsieve.jobscheduling.{ ETLLogContext, JobSchedulingPorts }
import com.pennsieve.service.utilities.ResponseLogger.logResponse
import com.pennsieve.service.utilities.{ ContextLogger, ResponseTier }

import scala.concurrent.{ ExecutionContext, Future }

class OrganizationsHandler(
  claim: Jwt.Claim
)(implicit
  ports: JobSchedulingPorts,
  ec: ExecutionContext,
  log: ContextLogger
) extends GuardrailHandler {

  type SetQuota = SetQuotaResponse
  type SetQuotaBuilder = OrganizationsResource.SetQuotaResponse.type
  implicit val setQuotaMarker: ResponseTier[SetQuota] =
    ResponseTier("SetQuota", "failed to set quota", "set quota")

  override def setQuota(
    respond: SetQuotaBuilder
  )(
    organizationId: Int,
    quota: Quota
  ): Future[SetQuota] = {
    implicit val logContext: ETLLogContext = ETLLogContext(organizationId = Some(organizationId))

    val hasPermissions =
      isServiceClaim(claim) && hasOrganizationAccess(claim, OrganizationId(organizationId))

    if (hasPermissions)
      ports.db
        .run(getOrganization(organizationId))
        .flatMap {
          _.fold {
            ports.db.run[Any](create(organizationId, quota.slotsAllowed))
          } { _ =>
            ports.db.run[Any](updateQuota(organizationId, quota.slotsAllowed))
          }
        }
        .map(_ => logResponse(respond.Created))
    else
      Future.successful(logResponse(respond.Forbidden))
  }
}

object OrganizationsHandler {
  def routes(
    implicit
    ports: JobSchedulingPorts,
    log: ContextLogger,
    system: ActorSystem,
    executionContext: ExecutionContext
  ): Route =
    authenticateJwt(system.name)(ports.jwt) { claim =>
      OrganizationsResource.routes(new OrganizationsHandler(claim))
    }
}
