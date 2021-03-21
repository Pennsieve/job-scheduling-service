// Copyright (c) [2018] - [2020] Blackfynn, Inc. All Rights Reserved.

package com.blackfynn.jobscheduling.db
import com.blackfynn.jobscheduling.JobSchedulingServiceSpecHarness
import com.blackfynn.jobscheduling.TestPayload.uploadPayload
import com.blackfynn.test.AwaitableImplicits
import org.scalatest.{ Matchers, WordSpec }

class PayloadsMapperSpec
    extends WordSpec
    with JobSchedulingServiceSpecHarness
    with AwaitableImplicits
    with Matchers {

  "PayloadsMapper" should {
    "create a new payload" in {
      val entry =
        ports.db
          .run(PayloadsMapper.create(uploadPayload, Some(uploadPayload.packageId)))
          .awaitFinite()

      ports.db.run(PayloadsMapper.get(entry.id)).awaitFinite().get shouldBe entry
    }
  }
}
