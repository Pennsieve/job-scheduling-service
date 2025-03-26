// Copyright (c) [2018] - [2025] Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.jobscheduling.clients

import akka.NotUsed
import akka.http.scaladsl.model.StatusCodes.NotFound
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{ Authorization, OAuth2BearerToken, RawHeader }
import akka.stream.scaladsl.Flow
import cats.implicits._
import com.pennsieve.auth.middleware.{ DatasetId, Jwt, OrganizationId, UserId }
import com.pennsieve.core.clients.packages.{ PackagesClient, UploadCompleteResponse }
import com.pennsieve.jobscheduling.Authenticator.generateServiceToken
import com.pennsieve.jobscheduling.clients.PennsieveApiClient.CreateAuthToken
import com.pennsieve.jobscheduling.clients.HttpClient.HttpClient
import com.pennsieve.jobscheduling.db.JobStateHelpers.toPackageState
import com.pennsieve.jobscheduling.errors.HttpClientUnsupportedResponse
import com.pennsieve.jobscheduling.model.EventualResult.{
  EitherContext,
  EventualResponseT,
  EventualResultT
}
import com.pennsieve.jobscheduling.model.{ ETLEvent, PackageId, PackageLostEvent }
import com.pennsieve.jobscheduling.{ PennsieveApiConfig, ThrottleConfig }
import com.pennsieve.models._
import com.pennsieve.service.utilities.{ ContextLogger, Tier }
import io.circe.Decoder
import io.circe.generic.semiauto.deriveDecoder
import io.circe.parser.decode

import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future }

case class PayloadCommonAttributes(packageId: Int, datasetId: Int, size: Long)

case class InvalidPayload(response: String) extends Exception

/*
 * Functions for updating package information in the Pennsieve API
 */
trait PennsieveApiClient {

  def setPackageState(
    organizationId: Int,
    state: PackageState,
    payload: Payload
  ): EventualResponseT[String] =
    setPackageState(organizationId, payload.datasetId, payload.packageId, state)

  def setPackageState(
    organizationId: Int,
    datasetId: Int,
    packageId: Int,
    state: PackageState
  ): EventualResponseT[String]

  def setPackageUploadComplete(
    packageId: PackageId,
    datasetId: DatasetId,
    organizationId: OrganizationId,
    userId: UserId,
    jobId: JobId
  )(implicit
    ec: ExecutionContext
  ): EventualResultT[UploadCompleteResponse]

  /*
   * Update package state and storage based on a given ETLEvent
   */
  def updatePackageFlow(
    throttle: ThrottleConfig
  )(implicit
    ec: ExecutionContext,
    log: ContextLogger,
    tier: Tier[_]
  ): Flow[EitherContext[(ETLEvent, Payload)], EitherContext[(ETLEvent, Payload)], NotUsed] =
    Flow[EitherContext[(ETLEvent, Payload)]]
      .throttle(throttle.parallelism, throttle.period)
      .mapAsyncUnordered[EitherContext[(ETLEvent, Payload)]](throttle.parallelism) {
        case Right((event, payload)) if payload.`type` == PayloadType.Append =>
          Future.successful[EitherContext[(ETLEvent, Payload)]]((event, payload).asRight)
        case Right((event, payload)) =>
          setPackageState(event.organizationId, toPackageState(event.jobState), payload)
            .map { _ =>
              log.tierContext.info(
                s"PennsieveApiClient set package state ${toPackageState(event.jobState)}"
              )(event.logContext)

              (event, payload)
            }
            .recover {
              case (NotFound, _) =>
                val lostEvent =
                  PackageLostEvent(
                    event.importId,
                    event.payloadId,
                    event.manifestUri,
                    event.organizationId,
                    event.userId,
                    event.receiptHandle,
                    event.sentAt
                  )

                (lostEvent, payload)
            }
            .leftMap {
              case (_, err) => (err, event.logContext)
            }
            .value

        case errorContext => Future.successful(errorContext)
      }
}

object PennsieveApiClient {
  type UpdatePackageFlow =
    Flow[EitherContext[(ETLEvent, Payload)], EitherContext[(ETLEvent, Payload)], NotUsed]
  type CreateAuthToken = (Int, Int) => Jwt.Token
}

class PennsieveApiClientImpl(
  apiConfig: PennsieveApiConfig,
  jwt: Jwt.Config,
  packagesClient: PackagesClient,
  httpClient: HttpClient
) extends PennsieveApiClient {

  /*
   * Create standard pennsieve api headers
   */
  private def createHeaders(organizationId: Int, datasetId: Int, createAuthToken: CreateAuthToken) =
    List(
      Authorization(OAuth2BearerToken(createAuthToken(organizationId, datasetId).value)),
      RawHeader("X-ORGANIZATION-INT-ID", organizationId.toString)
    )

  /*
   * Add headers to a request that are required by the pennsieve API
   */
  private def withApiHeaders(
    req: HttpRequest,
    organizationId: Int,
    datasetId: Int,
    createAuthToken: CreateAuthToken
  ): HttpRequest =
    req.withHeaders(createHeaders(organizationId, datasetId, createAuthToken))

  private def createAuthToken(organizationId: Int, datasetId: Int) =
    generateServiceToken(jwt, 5 minutes, organizationId, datasetId)

  override def setPackageState(
    organizationId: Int,
    datasetId: Int,
    packageId: Int,
    state: PackageState
  ): EventualResponseT[String] = {
    val req =
      HttpRequest(
        uri = s"${apiConfig.baseUrl}/packages/$packageId?updateStorage",
        method = HttpMethods.PUT,
        entity = HttpEntity(
          ContentType(MediaTypes.`application/json`),
          s"""{ "state": "${state.toString}" }""".stripMargin
        )
      )

    httpClient(withApiHeaders(req, organizationId, datasetId, createAuthToken))
  }

  override def setPackageUploadComplete(
    packageId: PackageId,
    datasetId: DatasetId,
    organizationId: OrganizationId,
    userId: UserId,
    jobId: JobId
  )(implicit
    ec: ExecutionContext
  ): EventualResultT[UploadCompleteResponse] =
    packagesClient
      .uploadComplete(
        Right(packageId.value),
        userId.value,
        createHeaders(organizationId.value, datasetId.value, createAuthToken)
      )
      .leftMap[Throwable] {
        case Right(unsupportedResponse) =>
          HttpClientUnsupportedResponse[PackagesClient](unsupportedResponse)

        case Left(err) => err
      }
}
