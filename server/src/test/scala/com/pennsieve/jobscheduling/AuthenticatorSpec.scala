// Copyright (c) [2018] - [2022] Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.jobscheduling
import com.pennsieve.auth.middleware.Jwt
import com.pennsieve.auth.middleware.Jwt.Config
import com.pennsieve.jobscheduling.handlers.AuthorizationChecks.{
  withAuthorization,
  withDatasetAccess
}
import com.pennsieve.test.AwaitableImplicits
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.EitherValues._

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

class AuthenticatorSpec extends AnyWordSpec with AwaitableImplicits with Matchers {

  "Authenticator" should {
    "generate a valid service claim" in {
      implicit val jwtConfig = new Config { override def key: String = "key" }

      val serviceToken =
        Authenticator.generateServiceToken(jwtConfig, 5 minutes, 1, 1)

      val claim = Jwt.parseClaim(serviceToken).value

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
