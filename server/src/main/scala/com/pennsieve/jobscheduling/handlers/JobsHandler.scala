// Copyright (c) [2018] - [2025] Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.jobscheduling.handlers

import java.io.{ PrintWriter, StringWriter }
import java.util.UUID
import akka.actor.ActorSystem
import akka.http.scaladsl.server.Route
import io.circe.syntax._
import cats.data.EitherT
import com.pennsieve.auth.middleware.AkkaDirective._
import com.pennsieve.auth.middleware.{ DatasetId, Jwt, OrganizationId, UserId }
import com.pennsieve.jobscheduling._
import com.pennsieve.jobscheduling.clients.Notifications
import com.pennsieve.jobscheduling.db._
import com.pennsieve.jobscheduling.errors._
import com.pennsieve.jobscheduling.handlers.AuthorizationChecks._
import com.pennsieve.service.utilities.ResponseLogger._
import com.pennsieve.service.utilities.{ ContextLogger, ResponseTier }
import cats.implicits._
import com.pennsieve.jobscheduling.commons.JobState
import com.pennsieve.jobscheduling.commons.JobState._
import com.pennsieve.jobscheduling.model.EventualResult.EventualResultT
import com.pennsieve.jobscheduling.model.JobConverters.RichJob
import com.pennsieve.jobscheduling.model.{ Cursor, InvalidCursorException, PackageId }
import com.pennsieve.jobscheduling.scheduler.JobScheduler
import com.pennsieve.jobscheduling.server.generated.definitions
import com.pennsieve.jobscheduling.server.generated.definitions.{ JobPage, UploadResult }
import com.pennsieve.jobscheduling.server.generated.jobs.JobsResource.{
  CompleteUploadResponse,
  CreateResponse,
  GetAllJobsResponse,
  GetAllJobsResponseOK,
  GetJobResponse,
  GetPackageJobsResponse,
  GetPackageStateResponse
}
import com.pennsieve.jobscheduling.server.generated.jobs.{
  JobsResource,
  JobsHandler => GuardrailHandler
}
import com.pennsieve.models.PackageState._
import com.pennsieve.models.{ JobId, Payload, _ }

import scala.concurrent.{ ExecutionContext, Future }
import scala.language.reflectiveCalls
import scala.util.control.NonFatal
import scala.util.{ Failure, Success, Try }
import com.pennsieve.jobscheduling.errors.ForbiddenException
import software.amazon.awssdk.services.sqs.model.SendMessageResponse

/**
  * The JobsHandler contains all HTTP endpoints that other services use to
  * communicate with JSS.
  */
class JobsHandler(
  claim: Jwt.Claim,
  ports: JobsHandlerPorts,
  jobScheduler: JobScheduler
)(implicit
  executionContext: ExecutionContext,
  log: ContextLogger
) extends GuardrailHandler {

  implicit val _ports: JobsHandlerPorts = ports

  private def toUUID(jobId: String): Future[UUID] =
    Try(UUID.fromString(jobId)) match {
      case Success(id) => Future.successful(id)
      case Failure(_) => Future.failed(InvalidJobIdException(jobId))
    }

  def parseJobId(jobId: String): Future[JobId] = toUUID(jobId).map(JobId.apply)

  type GetAllJobs = GetAllJobsResponse
  type GetAllOK = GetAllJobsResponseOK

  type GetAllRespond = GetAllJobsResponse.type

  implicit val getAllJobsMarker: ResponseTier[GetAllJobs] =
    ResponseTier("GetAllJobs", "failed to get all jobs with", "got all jobs")

  override def getAllJobs(respond: GetAllRespond)(organizationId: Int): Future[GetAllJobs] = {
    implicit val logContext: ETLLogContext = ETLLogContext(organizationId = Some(organizationId))

    def getAllJobsForOrganization: Future[GetAllJobs] =
      ports
        .getJobs(OrganizationId(organizationId))
        .map { jobs =>
          logResponse(respond.OK(jobs.toVector.map(_.toSwaggerJob)))
        }

    withAuthorization[GetAllJobsResponse](claim, organizationId) { _ =>
      ports
        .getOrganization(OrganizationId(organizationId))
        .flatMap {
          case Some(_) => getAllJobsForOrganization
          case None => Future.successful(logResponse(respond.NotFound))
        }
    }.recover {
      case ForbiddenException => logResponse(respond.Forbidden)
      case NonFatal(e) => logResponse(respond.InternalServerError(e.toString), e.toString)
    }
  }

  type GetJob = GetJobResponse
  implicit val getJobMarker: ResponseTier[GetJob] =
    ResponseTier("GetJob", "failed to get job with", "got job")

  override def getJob(
    respond: JobsResource.GetJobResponse.type
  )(
    organizationId: Int,
    jobId: String
  ): Future[GetJob] = {
    implicit val logContext: ETLLogContext =
      ETLLogContext(Try(JobId(UUID.fromString(jobId))).toOption, Some(organizationId))

    withAuthorization[GetJob](claim, organizationId) { _ =>
      for {
        jobId <- parseJobId(jobId)
        maybeOrganization <- ports.getOrganization(OrganizationId(organizationId))
        response <- maybeOrganization match {
          case Some(_) =>
            ports.getJob(jobId).map {
              case Some(job) => logResponse(respond.OK(job.toSwaggerJob))
              case None =>
                val msg = s"No job found for given id $jobId"
                logResponse(respond.NotFound(msg), msg)
            }
          case None =>
            val msg = s"No organization found for $organizationId"
            Future.successful(logResponse(respond.NotFound(msg), msg))
        }
      } yield response
    }.recover {
      case ForbiddenException => logResponse(respond.Forbidden)
      case NonFatal(e) => logResponse(respond.InternalServerError(e.toString), e.toString)
    }
  }

  type InternalServerError = JobsResource.CreateResponseInternalServerError
  type CreateResponse = JobsResource.CreateResponse
  implicit val createMarker: ResponseTier[CreateResponse] =
    ResponseTier("CreateJob", "failed to create job", "created job")

  override def create(
    respond: JobsResource.CreateResponse.type
  )(
    organizationId: Int,
    jobId: String,
    payload: Payload
  ): Future[CreateResponse] = {

    def createJob(
      jobId: JobId,
      payload: Payload,
      organizationId: Int,
      userId: Option[Int],
      respond: CreateResponse.type
    )(implicit
      log: ContextLogger,
      logContext: ETLLogContext
    ): EitherT[Future, CreateResponse, Job] =
      EitherT {
        ports
          .createJob(jobId, payload, OrganizationId(organizationId), userId.map(UserId))
      }.map { job =>
          log.tierContext[CreateResponse].info(s"JobsHandler wrote job to database")(job.logContext)
          job
        }
        .leftMap { error =>
          val msg = s"Failed to update database with ${error.getMessage}"
          logResponse(respond.InternalServerError(msg), msg)
        }

    def notifyConsumer(
      createdJob: Job,
      payload: Payload,
      respond: CreateResponse.type
    )(implicit
      log: ContextLogger,
      logContext: ETLLogContext
    ): EitherT[Future, CreateResponse, SendMessageResponse] = {
      JobSchedulingPorts
        .notifyUploadsConsumer(createdJob, payload)(ports.notifyUploadConsumer, executionContext)
        .leftFlatMap {
          case UnsupportedPayload(badPayload) =>
            val msg = s"Unsupported payload: $badPayload"
            EitherT.leftT[Future, SendMessageResponse](
              logResponse(respond.InternalServerError(msg), msg)
            )
          case error =>
            val msg = s"Failed to add manifest to queue with ${error.getMessage}"
            EitherT.leftT[Future, SendMessageResponse](
              logResponse(respond.InternalServerError(msg), msg)
            )
        }
        .map { messageResult =>
          log
            .tierContext[CreateResponse]
            .info(s"JobsHandler has been sent sent message to UploadsConsumer: $messageResult")(
              createdJob.logContext
            )
          messageResult
        }
    }

    def createAndNotify(
      jobId: JobId,
      organizationId: Int,
      payload: Payload,
      userId: => Option[UserId],
      respond: CreateResponse.type
    )(implicit
      log: ContextLogger,
      logContext: ETLLogContext
    ): Future[CreateResponse] = {
      createJob(jobId, payload, organizationId, userId.map(_.value), respond)
        .flatMap[CreateResponse, Job] { createdJob =>
          payload match {
            case _: Workflow =>
              EitherT(jobScheduler.addJob())
                .leftMap(_ => CreateResponse.InternalServerError("Job not queued"))
                .map(_ => createdJob)
            case uploadPayload =>
              notifyConsumer(createdJob, uploadPayload, respond)
                .map(_ => createdJob)
          }
        }
        .map(job => respond.Created(job.toSwaggerJob))
        .map(logResponse[CreateResponse])
        .valueOr(identity)
    }

    implicit val logContext: ETLLogContext =
      ETLLogContext(Try(JobId(UUID.fromString(jobId))).toOption, Some(organizationId))

    withAuthorization(claim, organizationId) { _ =>
      withDatasetAccess(claim, payload.datasetId) { _ =>
        for {
          validJobId <- parseJobId(jobId)
          maybeJob <- ports.getJob(validJobId)
          response <- maybeJob match {
            case Some(existingJob) =>
              Future.successful(respond.Accepted(existingJob.toSwaggerJob))
            case None =>
              createAndNotify(
                validJobId,
                organizationId,
                payload,
                Some(UserId(payload.userId)),
                respond
              )
          }
        } yield response
      }
    }.recover {
      case ForbiddenException => logResponse(CreateResponse.Forbidden)
    }
  }

  type GetPackageJobs = GetPackageJobsResponse
  implicit val getPackageJobs: ResponseTier[GetPackageJobs] =
    ResponseTier[GetPackageJobs](
      "GetPackageJob",
      "failed to get jobs for package",
      "got packages for jobs"
    )

  override def getPackageJobs(
    respond: GetPackageJobsResponse.type
  )(
    datasetId: Int,
    packageId: Int,
    pageSize: Int = 100,
    cursor: Option[String] = None
  ): Future[GetPackageJobs] =
    withDatasetAccess(claim, datasetId) { _ =>
      {
        for {
          cursor <- {
            EitherT
              .fromEither[Future] {
                cursor match {
                  case Some(cursorString) =>
                    Cursor.fromString(cursorString).map(_.some)
                  case None => Option.empty[Cursor].asRight[Throwable]
                }
              }
          }

          response <- {
            ports
              .getJobsForPackage(PackageId(packageId), pageSize, cursor)
              .map { jobs =>
                if (jobs.length > pageSize) {
                  val nextPageFirstJob = jobs.last
                  val cursor = Cursor(nextPageFirstJob.id, nextPageFirstJob.createdAt).toString

                  respond.OK(JobPage(jobs.dropRight(1).map(_.toSwaggerJob).toVector, Some(cursor)))
                } else {
                  respond.OK(JobPage(jobs.map(_.toSwaggerJob).toVector))
                }
              }
          }
        } yield response
      }.valueOr {
        case InvalidCursorException => respond.BadRequest(InvalidCursorException.getMessage)
        case e => respond.InternalServerError(e.getMessage)
      }
    }.recover {
      case ForbiddenException => respond.Forbidden
    }

  type GetPackageState = GetPackageStateResponse
  implicit val getPackageState: ResponseTier[GetPackageState] =
    ResponseTier[GetPackageState](
      "GetPackageState",
      "failed to get state for package",
      "got package state from jobs"
    )

  def inferPackageStateFromJobState(jobstate: JobState): (PackageState) = {
    jobstate match {
      case NotProcessing => UPLOADED
      case Succeeded => READY
      case Failed => PROCESSING_FAILED
      case Lost => PROCESSING_FAILED
      case _ => UNAVAILABLE
    }
  }
  override def getPackageState(
    respond: GetPackageStateResponse.type
  )(
    organizationId: Int,
    datasetId: Int,
    packageId: Int
  ): Future[GetPackageState] = {
    implicit val logContext: ETLLogContext = ETLLogContext(organizationId = Some(organizationId))
    withDatasetAccess(claim, datasetId) { _ =>
      for {
        maybeOrganization <- ports.getOrganization(OrganizationId(organizationId))
        response <- maybeOrganization match {
          case Some(_) =>
            ports
              .getLastJobNotAppend(datasetId, packageId, organizationId)
              .map {
                case Some(job) =>
                  logResponse(respond.OK(inferPackageStateFromJobState(job.state).asJson))

                case None =>
                  val msg = s"No job found for given package id $packageId"
                  logResponse(respond.NotFound, msg)
              }
          case None =>
            val msg = s"No organization found for $organizationId"
            Future.successful(logResponse(respond.NotFound, msg))
        }
      } yield response
    }
  }.recover {
    case ForbiddenException => respond.Forbidden
  }

  type CompleteUpload = CompleteUploadResponse
  implicit val completeUploadMarker: ResponseTier[CompleteUpload] =
    ResponseTier[CompleteUpload](
      "CompleteUpload",
      "failed to complete upload",
      "upload completed without processing"
    )

  override def completeUpload(
    respond: CompleteUploadResponse.type
  )(
    organizationId: Int,
    jobId: String,
    uploadResult: definitions.UploadResult
  ): Future[CompleteUpload] = {
    def notifyUserAndAPI(
      job: Job,
      payload: Upload,
      isUploadSuccessful: Boolean
    )(implicit
      executionContext: ExecutionContext,
      logContext: ETLLogContext,
      ports: JobsHandlerPorts
    ): EitherT[Future, Throwable, Job] = {
      for {
        foundJob <- EitherT {
          ports
            .getJob(job.id)
            .transform {
              case Success(maybeJob) =>
                Success(maybeJob.toRight[Throwable](NoJobException(job.id)))
              case Failure(e) => Success(DatabaseException(e).asLeft)
            }
        }
        _ <- {
          if (isUploadSuccessful)
            ports.packageUploadComplete(
              OrganizationId(job.organizationId),
              DatasetId(payload.datasetId),
              PackageId(payload.packageId),
              UserId(payload.userId),
              foundJob.id
            )
          else
            ports.updatePackageState(OrganizationId(organizationId), UPLOAD_FAILED, payload, job.id)
        }
        _ = log.tierContext[CompleteUpload].info("finished calling packages upload-complete")

      } yield foundJob
    }

    def completeNotProcessing(
      uploadResult: UploadResult,
      job: Job,
      payload: Payload
    )(implicit
      logContext: ETLLogContext
    ): EventualResultT[Job] = {
      for {
        parsedPayload <- {
          payload match {
            case upload: Upload => EitherT.rightT[Future, Throwable](upload)
            case _ =>
              EitherT.leftT[Future, Upload](UnsupportedUploadPayload(job, payload))
          }
        }

        job <- {
          notifyUserAndAPI(
            job = job,
            payload = parsedPayload,
            isUploadSuccessful = uploadResult.isSuccess
          )
        }
      } yield job
    }

    // get job and payload
    parseJobId(jobId)
      .flatMap { parsedJobId =>
        implicit val logContext: ETLLogContext =
          ETLLogContext(Some(parsedJobId), Some(organizationId))
        ports
          .getJobWithPayload(parsedJobId)
          .subflatMap {
            case None => NoJobException(parsedJobId).asLeft
            case Some(job) => job.asRight
          }
          .flatMap[Throwable, Job] {
            case (job, PayloadEntry(payload, _, _, _, _)) if job.state == Uploading =>
              completeNotProcessing(uploadResult, job, payload)

            case (job, _) if job.state == NotProcessing =>
              EitherT.leftT[Future, Job](SameStateTransitionIgnoring)

            case (job, _) if JobState.terminalStates contains job.state =>
              EitherT.leftT[Future, Job](TerminalStateTransitionIgnoring)

            case (job, _) =>
              EitherT.leftT[Future, Job](InvalidJobState(job))
          }
          .map(_ => respond.OK)
          .leftMap {
            case _: SameStateTransitionIgnoring.type =>
              logResponse(respond.Accepted)
            case _: TerminalStateTransitionIgnoring.type =>
              logResponse(respond.Accepted)
            case _: NoJobException => logResponse(respond.NotFound)
            case error: UnsupportedUploadPayload =>
              logResponse(respond.BadRequest(error.getMessage), error.getMessage)
            case error: InvalidStateUpdate =>
              logResponse(respond.BadRequest(error.getMessage), error.getMessage)
            case error: InvalidJobState =>
              logResponse(respond.BadRequest(error.getMessage), error.getMessage)
            case error =>
              val sw = new StringWriter
              error.printStackTrace(new PrintWriter(sw))
              log.tierContext[CompleteUpload].error(sw.toString)
              logResponse(respond.InternalServerError(error.getMessage), error.getMessage)
          }
          .valueOr(identity)
      }
      .recover {
        case InvalidJobIdException(invalidJobId) =>
          implicit val logContext: ETLLogContext =
            ETLLogContext(None, Some(organizationId))
          val msg = s"JobId was not a UUID: $invalidJobId"
          logResponse(respond.BadRequest(msg), msg)
      }
  }
}

object JobsHandler {
  def routes(
    ports: JobsHandlerPorts,
    jobScheduler: JobScheduler
  )(implicit
    system: ActorSystem,
    executionContext: ExecutionContext,
    log: ContextLogger
  ): Route =
    authenticateJwt(system.name)(ports.jwt) { claim =>
      JobsResource.routes(new JobsHandler(claim, ports, jobScheduler))
    }
}
