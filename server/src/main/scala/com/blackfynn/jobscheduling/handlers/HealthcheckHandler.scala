// Copyright (c) [2018] - [2021] Pennsieve, Inc. All Rights Reserved.

package com.blackfynn.jobscheduling.handlers

import akka.stream.ActorMaterializer
import com.blackfynn.jobscheduling.JobSchedulingPorts
import com.blackfynn.jobscheduling.server.generated.healthcheck.{
  HealthcheckResource,
  HealthcheckHandler => GuardrailHandler
}
import com.blackfynn.jobscheduling.db.profile.api._
import slick.dbio.{ DBIOAction, Effect, NoStream }

import scala.concurrent.{ ExecutionContext, Future }

class HealthcheckHandler(
  ports: JobSchedulingPorts
)(implicit
  executionContext: ExecutionContext
) extends GuardrailHandler {

  private def checkDatabaseHealth: DBIOAction[Int, NoStream, Effect.Read] = {
    sql"""SELECT 1 AS check""".as[Int].head
  }

  override def healthcheck(
    respond: HealthcheckResource.healthcheckResponse.type
  )(
  ): Future[HealthcheckResource.healthcheckResponse] = {
    ports.db
      .run(checkDatabaseHealth)
      .map { _ =>
        HealthcheckResource.healthcheckResponseOK
      }
      .recoverWith {
        case e => Future.successful(respond.ServiceUnavailable(e.toString))
      }
  }
}

object HealthcheckHandler {
  def routes(
  )(implicit
    materializer: ActorMaterializer,
    executionContext: ExecutionContext,
    ports: JobSchedulingPorts
  ) = HealthcheckResource.routes(new HealthcheckHandler(ports))
}
