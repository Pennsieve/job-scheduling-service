// Copyright (c) [2018] - [2022] Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.jobscheduling.clients
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{ HttpRequest, StatusCode }
import akka.stream.Materializer
import cats.data.EitherT
import cats.implicits._

import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future }

object HttpClient {

  type HttpClient = HttpRequest => EitherT[Future, (StatusCode, Throwable), String]

  def sendHttpRequest(
    req: HttpRequest
  )(implicit
    system: ActorSystem,
    mat: Materializer,
    ec: ExecutionContext
  ): EitherT[Future, (StatusCode, Throwable), String] =
    EitherT[Future, (StatusCode, Throwable), String] {
      for {
        resp <- Http().singleRequest(req)
        body <- resp.entity.toStrict(10.seconds)
      } yield
        if (resp.status.isSuccess()) body.data.utf8String.asRight
        else (resp.status, new Exception(body.data.utf8String)).asLeft
    }

  def apply(
  )(implicit
    system: ActorSystem,
    mat: Materializer,
    ec: ExecutionContext
  ): HttpClient =
    sendHttpRequest(_)

}
