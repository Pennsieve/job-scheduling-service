// Copyright (c) [2018] - [2020] Blackfynn, Inc. All Rights Reserved.

package com.blackfynn.jobscheduling.handlers

import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import com.blackfynn.jobscheduling.{
  JobSchedulingServiceSpecHarness,
  UnhealthyDBJobSchedulingServiceSpecHarness
}
import com.blackfynn.test.AwaitableImplicits
import com.blackfynn.jobscheduling.clients.generated.healthcheck.HealthcheckClient
import com.blackfynn.jobscheduling.clients.generated.healthcheck.HealthcheckResponse
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
