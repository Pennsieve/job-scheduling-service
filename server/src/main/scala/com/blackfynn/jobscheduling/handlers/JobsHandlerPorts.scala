// Copyright (c) [2018] - [2021] Pennsieve, Inc. All Rights Reserved.

package com.blackfynn.jobscheduling.handlers

import akka.http.scaladsl.model.StatusCodes.NotFound
import cats.data.EitherT
import cats.implicits._
import com.blackfynn.auth.middleware.{ DatasetId, Jwt, OrganizationId, UserId }
import com.blackfynn.core.clients.packages.UploadCompleteResponse
import com.blackfynn.jobscheduling.JobSchedulingPorts.{ createGetJob, GetJob }
import com.blackfynn.jobscheduling._
import com.blackfynn.jobscheduling.clients.PennsieveApiClient
import com.blackfynn.jobscheduling.clients.SQSClient.{ createSendMessage, SendMessage }
import com.blackfynn.jobscheduling.commons.JobState
import com.blackfynn.jobscheduling.commons.JobState.{ Cancelled, NoTaskId }
import com.blackfynn.jobscheduling.db.profile.api._
import com.blackfynn.jobscheduling.db.{ OrganizationQuotaMapper, _ }
import com.blackfynn.jobscheduling.errors.PackagesUploadCompleteError
import com.blackfynn.jobscheduling.handlers.JobsHandlerPorts._
import com.blackfynn.jobscheduling.model.EventualResult.{
  EventualResponseT,
  EventualResult,
  EventualResultT
}
import com.blackfynn.jobscheduling.model.{ Cursor, PackageId }
import com.blackfynn.models.{ JobId, PackageState, Payload, Workflow }
import com.blackfynn.service.utilities.ContextLogger

import scala.concurrent.{ ExecutionContext, Future }
import scala.util.control.NonFatal
import scala.util.{ Failure, Success }

case class JobsHandlerPorts(
  createJob: CreateJob,
  notifyUploadConsumer: NotifyUpload,
  notifyUser: NotifyUser,
  getJobs: GetJobs,
  getOrganization: GetOrganization,
  getJob: GetJob,
  setJobState: SetJobState,
  getLastJobNotAppend: GetLastJobNotAppend,
  getJobWithPayload: GetJobWithPayload,
  packageUploadComplete: PackageUploadComplete,
  getJobsForPackage: GetJobsForPackage,
  updatePackageState: UpdatePackageState,
  jwt: Jwt.Config
)

object JobsHandlerPorts {

  type CreateJob = (JobId, Payload, OrganizationId, Option[UserId]) => EventualResult[Job]
  type UploadComplete = (JobId, PackageId, Payload, Boolean) => EventualResult[Unit]
  type NotifyUpload = SendMessage
  type NotifyUser = SendMessage
  type GetJobs = OrganizationId => Future[Seq[Job]]
  type GetOrganization = OrganizationId => Future[Option[OrganizationQuota]]
  type SetJobState = (JobId, JobState with NoTaskId) => EventualResultT[Unit]
  type GetJobWithPayload = JobId => EventualResultT[Option[(Job, PayloadEntry)]]
  type PackageUploadComplete =
    (OrganizationId, DatasetId, PackageId, UserId, JobId) => EventualResultT[Unit]
  type GetLastJobNotAppend = (Int, Int, Int) => Future[Option[Job]]
  type GetJobsForPackage = (PackageId, Int, Option[Cursor]) => EventualResultT[Seq[Job]]
  type UpdatePackageState =
    (OrganizationId, PackageState, Payload, JobId) => EventualResultT[Unit]

  def getJobs(db: Database): GetJobs =
    organizationId => db.run(JobsMapper.getAllJobs(organizationId))

  def createJob(db: Database)(implicit ec: ExecutionContext): CreateJob = {
    (jobId, payload, organizationId, userId) =>
      val state = {
        payload match {
          // workflow payloads should go directly to Available
          case _: Workflow =>
            JobState.Available
          // all other payload types should go to Uploading
          case _ =>
            JobState.Uploading
        }
      }

      db.run(
          JobsMapper
            .createWithPayload(
              jobId = jobId,
              payload = payload,
              organizationId = organizationId,
              state = state,
              userId = userId
            )
        )
        .map[Either[Throwable, Job]](Right(_))
        .recover {
          case NonFatal(e) => Left(e)
        }
  }

  def getOrganization(db: Database): GetOrganization =
    organizationId => db.run(OrganizationQuotaMapper.getOrganization(organizationId.value))

  def setJobState(db: Database)(implicit ec: ExecutionContext): SetJobState =
    (jobId, jobState) =>
      EitherT {
        db.run(JobsMapper.updateState(jobId, jobState))
          .transform {
            case Success(_) => Success(().asRight[Throwable])
            case Failure(e) => Success(e.asLeft[Unit])
          }
      }

  def getJobWithPayload(db: Database)(implicit ec: ExecutionContext): GetJobWithPayload =
    jobId =>
      EitherT {
        db.run(JobsMapper.getWithPayloadEntry(jobId))
          .transform {
            case Success(jobWithPayload) => Success(jobWithPayload.asRight[Throwable])
            case Failure(e) => Success(e.asLeft)
          }
      }

  def packageUploadComplete(
    pennsieveApiClient: PennsieveApiClient,
    db: Database
  )(implicit
    ec: ExecutionContext
  ): PackageUploadComplete =
    (organizationId, datasetId, packageId, userId, jobId) => {
      pennsieveApiClient
        .setPackageUploadComplete(packageId, datasetId, organizationId, userId, jobId)
        .flatMap[Throwable, Unit] {
          case UploadCompleteResponse.OK =>
            EitherT.rightT[Future, Throwable](())

          case UploadCompleteResponse.NotFound =>
            EitherT {
              db.run(JobsMapper.updateState(jobId, Cancelled))
                .transform {
                  case Success(_) => Success(().asRight[Throwable])
                  case Failure(e) => Success(e.asLeft[Unit])
                }
            }

          case error =>
            EitherT.leftT[Future, Unit](PackagesUploadCompleteError(error))
        }
    }

  def updatePackageState(
    pennsieveApiClient: PennsieveApiClient,
    db: Database
  )(implicit
    ec: ExecutionContext
  ): UpdatePackageState =
    (organizationId, packageState, payload, jobId) =>
      pennsieveApiClient
        .setPackageState(organizationId.value, packageState, payload)
        .map(_ => ())
        .leftFlatMap {
          case (statusCode, _) if statusCode == NotFound =>
            EitherT {
              db.run(JobsMapper.updateState(jobId, Cancelled))
                .transform {
                  case Success(_) => Success(().asRight[Throwable])
                  case Failure(e) => Success(e.asLeft[Unit])
                }
            }

          case (_, e) => EitherT.leftT[Future, Unit](e)
        }

  def getJobsForPackage(db: Database)(implicit ec: ExecutionContext): GetJobsForPackage =
    (packageId, pageSize, cursor) =>
      EitherT {
        db.run(JobsMapper.getJobsByPackageId(packageId.value, pageSize, cursor))
          .map(_.asRight[Throwable])
          .recover {
            case e => e.asLeft
          }
      }

  def getLastJobNotAppend(
    db: Database
  )(implicit
    ec: ExecutionContext
  ): GetLastJobNotAppend =
    (datasetId, packageId, organizationId) =>
      db.run(JobsMapper.getLastJobNotAppend(datasetId, packageId, organizationId))

  def apply(
    config: JobsHandlerConfig,
    pennsieveApiClient: PennsieveApiClient
  )(implicit
    ports: JobSchedulingPorts,
    log: ContextLogger,
    ec: ExecutionContext
  ): JobsHandlerPorts =
    JobsHandlerPorts(
      createJob = createJob(ports.db),
      notifyUploadConsumer = createSendMessage(ports.sqsClient, config.uploadsConsumer.queueName),
      notifyUser = createSendMessage(ports.sqsClient, config.notifications.queueName),
      getJobs = getJobs(ports.db),
      getOrganization = getOrganization(ports.db),
      getJob = createGetJob(ports.db),
      setJobState = setJobState(ports.db),
      getJobWithPayload = getJobWithPayload(ports.db),
      getLastJobNotAppend = getLastJobNotAppend(ports.db),
      packageUploadComplete = packageUploadComplete(pennsieveApiClient, ports.db),
      getJobsForPackage = getJobsForPackage(ports.db),
      updatePackageState = updatePackageState(pennsieveApiClient, ports.db),
      jwt = ports.jwt
    )
}
