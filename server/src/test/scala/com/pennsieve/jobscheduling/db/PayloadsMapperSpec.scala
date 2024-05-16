// Copyright (c) [2018] - [2022] Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.jobscheduling.db
import com.pennsieve.jobscheduling.JobSchedulingServiceSpecHarness
import com.pennsieve.jobscheduling.TestPayload.uploadPayload
import com.pennsieve.test.AwaitableImplicits
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class PayloadsMapperSpec
    extends AnyWordSpec
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
