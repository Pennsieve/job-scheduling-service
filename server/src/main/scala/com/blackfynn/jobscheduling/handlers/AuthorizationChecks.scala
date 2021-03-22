// Copyright (c) [2018] - [2021] Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.jobscheduling.handlers

import com.pennsieve.auth.middleware.Validator.{ hasDatasetAccess, hasOrganizationAccess }
import com.pennsieve.auth.middleware.{ DatasetId, DatasetPermission, Jwt, OrganizationId }
import com.pennsieve.jobscheduling.errors.ForbiddenException

import scala.concurrent.Future

object AuthorizationChecks {

  /*
   * Ensure that this claim has access to the given dataset
   */
  def withDatasetAccess[T](claim: Jwt.Claim, datasetId: Int)(f: Unit => Future[T]): Future[T] =
    if (hasDatasetAccess(claim, DatasetId(datasetId), DatasetPermission.CreateDeleteFiles)) f(Unit)
    else Future.failed(ForbiddenException)

  /*
   * Ensure that this claim has access to the given organization
   */
  def withAuthorization[T](claim: Jwt.Claim, organizationId: Int)(f: Unit => Future[T]): Future[T] =
    if (hasOrganizationAccess(claim, OrganizationId(organizationId))) f(Unit)
    else Future.failed(ForbiddenException)
}
