// Copyright (c) [2018] - [2021] Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.jobscheduling.handlers

import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import com.pennsieve.jobscheduling.{
  JobSchedulingServiceSpecHarness,
  UnhealthyDBJobSchedulingServiceSpecHarness
}
import com.pennsieve.test.AwaitableImplicits
import com.pennsieve.jobscheduling.clients.generated.healthcheck.HealthcheckClient
import com.pennsieve.jobscheduling.clients.generated.healthcheck.HealthcheckResponse
import org.scalatest.{ BeforeAndAfterEach, Matchers, WordSpec }

class HealthyHealthcheckHandlerSpec
    extends WordSpec
    with ScalatestRouteTest
    with JobSchedulingServiceSpecHarness
    with AwaitableImplicits
    with Matchers
    with BeforeAndAfterEach {

  def createRoutes: Route = Route.seal(HealthcheckHandler.routes())

  def createClient(routes: Route) = HealthcheckClient.httpClient(Route.asyncHandler(routes))

  "GET /health/" should {
    "return 200 if healthy" in {
      val client = createClient(createRoutes)
      val response = client.healthcheck().awaitFinite().right.get
      response shouldBe HealthcheckResponse.OK
    }
  }
}

class UnhealthyHealthcheckHandlerSpec
    extends WordSpec
    with ScalatestRouteTest
    with UnhealthyDBJobSchedulingServiceSpecHarness
    with AwaitableImplicits
    with Matchers
    with BeforeAndAfterEach {

  def createRoutes: Route = Route.seal(HealthcheckHandler.routes())

  def createClient(routes: Route) = HealthcheckClient.httpClient(Route.asyncHandler(routes))

  "GET /health/" should {
    "return 503 if unhealthy" in {
      val client = createClient(createRoutes)
      val response = client.healthcheck().awaitFinite().right.get
      response shouldBe a[HealthcheckResponse.ServiceUnavailable]
    }
  }
}
