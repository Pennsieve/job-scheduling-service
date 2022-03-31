// Copyright (c) [2018] - [2022] Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.jobscheduling.clients
import com.amazonaws.AmazonWebServiceRequest
import com.amazonaws.handlers.{ AsyncHandler => AWSAsyncHandler }
import com.pennsieve.jobscheduling.model.EventualResult.EventualResult

import scala.concurrent.Promise
import java.util.concurrent.{ Future => JavaFuture }

class AwsAsyncCallback[Request <: AmazonWebServiceRequest, Result]
    extends AWSAsyncHandler[Request, Result] {

  val promise: Promise[Either[Exception, Result]] =
    Promise[Either[Exception, Result]]

  override def onError(exception: Exception): Unit =
    promise.success(Left(exception))

  override def onSuccess(request: Request, result: Result): Unit =
    promise.success(Right(result))

}

object AwsAsyncCallback {
  def async[Request <: AmazonWebServiceRequest, Result](
    f: (Request, AWSAsyncHandler[Request, Result]) => JavaFuture[Result]
  ): Request => EventualResult[Result] =
    (request: Request) => {
      val awsAsyncCallback = new AwsAsyncCallback[Request, Result]
      f(request, awsAsyncCallback)
      awsAsyncCallback.promise.future
    }
}
