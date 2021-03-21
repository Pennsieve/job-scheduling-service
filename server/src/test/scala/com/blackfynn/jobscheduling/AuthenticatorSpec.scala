// Copyright (c) [2018] - [2020] Blackfynn, Inc. All Rights Reserved.

package com.blackfynn.jobscheduling
import com.blackfynn.auth.middleware.Jwt
import com.blackfynn.auth.middleware.Jwt.Config
import com.blackfynn.jobscheduling.handlers.AuthorizationChecks.{
  withAuthorization,
  withDatasetAccess
}
import com.blackfynn.test.AwaitableImplicits
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
