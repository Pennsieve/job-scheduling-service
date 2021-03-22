// Copyright (c) [2018] - [2021] Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.jobscheduling
import com.pennsieve.auth.middleware.Jwt
import com.pennsieve.auth.middleware.Jwt.Config
import com.pennsieve.jobscheduling.handlers.AuthorizationChecks.{
  withAuthorization,
  withDatasetAccess
}
import com.pennsieve.test.AwaitableImplicits
import org.scalatest.{ Matchers, WordSpecLike }

import scala.concurrent.duration.DurationInt
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class AuthenticatorSpec extends WordSpecLike with AwaitableImplicits with Matchers {

  "Authenticator" should {
    "generate a valid service claim" in {
      implicit val jwtConfig = new Config { override def key: String = "key" }

      val serviceToken =
        Authenticator.generateServiceToken(jwtConfig, 5 minutes, 1, 1)

      val claim = Jwt.parseClaim(serviceToken).right.get

      val maybeOne =
        withAuthorization(claim, 1) { _ =>
          withDatasetAccess(claim, 1) { _ =>
            Future.successful(1)
          }
        }

      maybeOne.awaitFinite() shouldBe 1
    }
  }

}
