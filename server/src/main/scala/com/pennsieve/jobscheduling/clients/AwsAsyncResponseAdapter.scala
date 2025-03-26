// Copyright (c) [2018] - [2025] Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.jobscheduling.clients

import scala.jdk.FutureConverters._
import scala.jdk.FunctionConverters._
import com.pennsieve.jobscheduling.model.EventualResult.EventualResult
import software.amazon.awssdk.awscore.{ AwsRequest, AwsResponse }

import java.util.concurrent.CompletableFuture
import java.util.function.{ BiFunction, Consumer }

object AwsAsyncResponseAdapter {

  def toEitherAsyncHandler[Response <: AwsResponse](
    response: Response,
    error: Throwable
  ): Either[Throwable, Response] = {
    if (error != null) {
      Left(error)
    } else {
      Right(response)
    }
  }

  def toEventualResult[Response <: AwsResponse](
    completableFuture: CompletableFuture[Response]
  ): EventualResult[Response] = {
    completableFuture.handleAsync(toEitherAsyncHandler _).asScala
  }

  def createRequestFunction[Request <: AwsRequest, Response <: AwsResponse](
    makeRequest: Request => CompletableFuture[Response]
  ): Request => EventualResult[Response] = makeRequest andThen toEventualResult

  def createRequestFunction[ArgType, Builder <: AwsRequest.Builder, Response <: AwsResponse](
    buildRequest: ArgType => Consumer[Builder],
    makeRequest: Consumer[Builder] => CompletableFuture[Response]
  ): ArgType => EventualResult[Response] =
    buildRequest andThen makeRequest andThen toEventualResult

  def createRequestFunction[Builder <: AwsRequest.Builder, Response <: AwsResponse](
    buildRequest: Consumer[Builder],
    makeRequest: Consumer[Builder] => CompletableFuture[Response]
  ): () => EventualResult[Response] = () => toEventualResult(makeRequest(buildRequest))
}
