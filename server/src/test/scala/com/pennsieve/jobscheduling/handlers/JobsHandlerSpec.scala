// Copyright (c) [2018] - [2025] Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.jobscheduling.handlers

import java.time.OffsetDateTime
import java.time.ZoneOffset.UTC
import java.util.UUID
import akka.actor.Scheduler
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{ Authorization, OAuth2BearerToken }
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import cats.data.EitherT
import cats.implicits._
import software.amazon.awssdk.services.sqs.model.SendMessageResponse
import com.pennsieve.auth.middleware.Jwt.Role.RoleIdentifier
import com.pennsieve.auth.middleware.{ DatasetId, Jwt, OrganizationId, UserClaim, UserId }
import com.pennsieve.core.clients.packages.UploadCompleteResponse
import com.pennsieve.jobscheduling.Fakes.{
  fakePennsieveApiClient,
  SetPackageState,
  SetUploadComplete
}
import com.pennsieve.jobscheduling.TestTask.taskId
import com.pennsieve.jobscheduling._
import com.pennsieve.jobscheduling.clients.PennsieveApiClient
import com.pennsieve.jobscheduling.clients.SQSClient.{ MessageBody, SendMessage }
import com.pennsieve.jobscheduling.clients.generated.definitions.{
  JobPage,
  UploadResult,
  Job => SwaggerJob
}
import com.pennsieve.jobscheduling.clients.generated.jobs._
import com.pennsieve.jobscheduling.commons.JobState
import com.pennsieve.jobscheduling.commons.JobState._
import com.pennsieve.jobscheduling.db.JobsMapper.get
import com.pennsieve.jobscheduling.db._
import com.pennsieve.jobscheduling.db.profile.api._
import com.pennsieve.jobscheduling.handlers.JobsHandlerPorts.{ apply => _, _ }
import com.pennsieve.jobscheduling.model.Cursor
import com.pennsieve.jobscheduling.model.JobConverters._
import com.pennsieve.jobscheduling.scheduler.JobScheduler
import com.pennsieve.jobscheduling.scheduler.JobSchedulerFakes.{
  runScheduler,
  schedulerAndRunnableEvent
}
import com.pennsieve.models.Manifest._
import com.pennsieve.models.{
  Channel,
  ETLAppendWorkflow,
  FileType,
  JobId,
  Manifest,
  PackageState,
  PackageType,
  Role,
  Upload,
  Workflow
}
import io.circe.Error
import io.circe.parser.decode
import io.circe.syntax._
import org.scalatest.EitherValues._
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import shapeless.syntax.inject.InjectSyntax
import software.amazon.awssdk.awscore.exception.{ AwsErrorDetails, AwsServiceException }

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.concurrent.{ Future, TimeoutException }

class JobsHandlerSpec
    extends AnyWordSpec
    with ScalatestRouteTest
    with JobSchedulingServiceSpecHarness
    with Matchers
    with BeforeAndAfterEach {

  implicit val scheduler: Scheduler = system.scheduler

  import com.pennsieve.jobscheduling.TestPayload._

  val forbiddenOrganization: Int = organizationId + 1
  val claim: Jwt.Claim =
    generateClaim(UserId(userId), DatasetId(datasetId), OrganizationId(organizationId))

  val token: Jwt.Token = Jwt.generateToken(claim)(ports.jwt)

  val authToken = List(Authorization(OAuth2BearerToken(token.value)))
  val successfulSendMessage: SendMessage = { _ =>
    Future.successful(Right(SendMessageResponse.builder().build()))
  }

  override def beforeEach(): Unit = {
    ports.db.run(JobsMapper.delete).awaitFinite()
    ports.db.run(OrganizationQuotaMapper.delete).awaitFinite()
    ports.db.run(OrganizationQuotaMapper.create(organizationId, 10)).awaitFinite()

    super.beforeEach()
  }

  "POST /organizations/:organizationId/jobs/:importId" should {

    "create a job and return the new job" in {
      val (jobScheduler, _) = runScheduler()

      val client = createClient(createRoutes(jobScheduler))

      val response: SwaggerJob = client
        .create(organizationId, createJobIdString(), uploadPayload, authToken)
        .awaitFinite()
        .value
        .getOrFail[CreateResponse.Created, SwaggerJob]

      // the created job should be in the database
      val job: Job = ports.db.run(get(JobId(response.id))).awaitFinite().get

      // the payload should be in the database as well
      val entry: PayloadEntry =
        ports.db.run(PayloadsMapper.get(job.payloadId)).awaitFinite().get

      job.state shouldBe Uploading
      job.organizationId shouldBe organizationId

      entry.payload shouldBe uploadPayload
    }

    "notify uploads consumer that a job has arrived" in {
      var sentManifest: Option[Manifest] = None

      val savingManifestUploadNotifier: NotifyUpload =
        string => {
          sentManifest = Some(decode[Manifest](string.value).toOption.get)
          Future.successful(Right(SendMessageResponse.builder().build()))
        }

      val (jobScheduler, _) = runScheduler()

      val client = createClient(
        createRoutes(jobScheduler, notifyUploadConsumer = savingManifestUploadNotifier)
      )

      val response: SwaggerJob = client
        .create(organizationId, createJobIdString(), uploadPayload, authToken)
        .awaitFinite()
        .value
        .getOrFail[CreateResponse.Created, SwaggerJob]

      val job: Job = ports.db.run(JobsMapper.get(JobId(response.id))).awaitFinite().get

      val maybePayloadEntry = ports.db.run(PayloadsMapper.get(job.payloadId)).awaitFinite()
      val maybePayload = maybePayloadEntry.map(_.payload)

      val expectedManifest = payloadToManifest(job)(maybePayload).value

      sentManifest shouldBe Some(expectedManifest)
    }

    "store the job if it fails to contact upload consumer" in {
      val (jobScheduler, _) = runScheduler()

      val failingNotifyUploadConsumer: NotifyUpload =
        _ =>
          Future.successful(
            Left(
              AwsServiceException.builder
                .message("SQS is down")
                .build()
            )
          )

      val client =
        createClient(createRoutes(jobScheduler, notifyUploadConsumer = failingNotifyUploadConsumer))

      val expectedJobId = createJobId()

      val response = client
        .create(organizationId, expectedJobId.toString, uploadPayload, authToken)
        .awaitFinite()
        .value

      response shouldBe CreateResponse.InternalServerError(
        "Failed to add manifest to queue with SQS is down"
      )

      val createdJob = ports.db.run(JobsMapper.get(JobId(expectedJobId))).awaitFinite().value
      createdJob.id.value shouldBe expectedJobId
    }

    "return error when the database is not updated" in {
      val (jobScheduler, _) = runScheduler()

      val failingCreateJob: CreateJob =
        (_, _, _, _) => Future.successful(Left(new Exception("database down")))

      val client =
        createClient(createRoutes(jobScheduler, createJob = failingCreateJob))

      val expectedJobId = createJobId()

      val response = client
        .create(organizationId, expectedJobId.toString, uploadPayload, authToken)
        .awaitFinite()
        .value

      response shouldBe CreateResponse.InternalServerError(
        "Failed to update database with database down"
      )

      ports.db.run(JobsMapper.get(JobId(expectedJobId))).awaitFinite() shouldBe None
    }

    "return forbidden when not authorized to access the given organization" in {
      val (jobScheduler, _) = runScheduler()

      val client = createClient(createRoutes(jobScheduler))

      val response = client
        .create(
          forbiddenOrganization,
          createJobIdString(),
          uploadPayload,
          Nil :+ Authorization(OAuth2BearerToken(token.value))
        )
        .awaitFinite()
        .value

      response shouldBe CreateResponse.Forbidden
    }

    "return forbidden when not authorized to write to the dataset" in {
      val (jobScheduler, _) = runScheduler()

      val client = createClient(createRoutes(jobScheduler))

      val claimWithRead: Jwt.Claim =
        generateClaim(
          UserId(userId),
          DatasetId(datasetId),
          OrganizationId(organizationId),
          datasetRole = Role.Viewer
        )
      val readToken: Jwt.Token = Jwt.generateToken(claimWithRead)(ports.jwt)
      val response = client
        .create(
          organizationId,
          createJobIdString(),
          uploadPayload,
          Nil :+ Authorization(OAuth2BearerToken(readToken.value))
        )
        .awaitFinite()
        .value

      response shouldBe CreateResponse.Forbidden
    }

    "return accepted for a second attempt to add a job to database" in {
      val idString = createJobIdString()

      val (jobScheduler, _) = runScheduler()

      val client = createClient(createRoutes(jobScheduler))

      val initialReturnedJob: SwaggerJob = client
        .create(organizationId, idString, uploadPayload, authToken)
        .awaitFinite()
        .value
        .getOrFail[CreateResponse.Created, SwaggerJob]

      // the created job should be in the database
      val job: Job = ports.db.run(get(JobId(initialReturnedJob.id))).awaitFinite().get
      // the payload should be in the database as well
      val entry: PayloadEntry =
        ports.db.run(PayloadsMapper.get(job.payloadId)).awaitFinite().get

      val secondReturnedJob: SwaggerJob = client
        .create(organizationId, idString, uploadPayload, authToken)
        .awaitFinite()
        .value
        .getOrFail[CreateResponse.Accepted, SwaggerJob]

      val jobAfterSecondRequest = ports.db.run(get(JobId(secondReturnedJob.id))).awaitFinite().get

      val payloadEntryAfterSecondRequest =
        ports.db.run(PayloadsMapper.get(jobAfterSecondRequest.payloadId)).awaitFinite().get

      jobAfterSecondRequest shouldBe job
      payloadEntryAfterSecondRequest shouldBe entry
    }

    "should not notify uploads consumer for second attempt to create a job" in {
      val idString = createJobIdString()

      var sentManifests: Int = 0

      val countingManifestUploadNotifier: NotifyUpload =
        _ => {
          sentManifests += 1
          Future.successful(Right(SendMessageResponse.builder().build()))
        }

      val (jobScheduler, _) = runScheduler()

      val client = createClient(
        createRoutes(jobScheduler, notifyUploadConsumer = countingManifestUploadNotifier)
      )

      client
        .create(organizationId, idString, uploadPayload, authToken)
        .awaitFinite()
        .value

      client
        .create(organizationId, idString, uploadPayload, authToken)
        .awaitFinite()
        .value

      sentManifests shouldBe 1
    }

    "should mark Workflow payloads as Available immediately" in {

      val (jobScheduler, _) = schedulerAndRunnableEvent()

      val client = createClient(createRoutes(jobScheduler))

      List[Workflow](importPayload(), appendPayload).foreach { payload =>
        val jobId = createJobIdString()
        val response: SwaggerJob = client
          .create(organizationId, jobId, payload, authToken)
          .awaitFinite()
          .value
          .getOrFail[CreateResponse.Created, SwaggerJob]

        // the created job should be in the database
        val job: Job = ports.db.run(get(JobId(response.id))).awaitFinite().get

        // the payload should be in the database as well
        val entry: PayloadEntry =
          ports.db.run(PayloadsMapper.get(job.payloadId)).awaitFinite().get

        job.state shouldBe Available
        job.organizationId shouldBe organizationId

        entry.payload shouldBe payload
      }
    }
  }

  "PUT /organizations/:organizationId/jobs/:jobId/upload-complete" should {

    "return bad request with invalid job id" in {
      val (jobScheduler, _) = schedulerAndRunnableEvent()

      val client = createClient(createRoutes(jobScheduler))

      val badJobId = "dummy-job-id"

      val response = client
        .completeUpload(
          organizationId,
          badJobId,
          UploadResult(isSuccess = true),
          Nil :+ Authorization(OAuth2BearerToken(token.value))
        )
        .awaitFinite()
        .value

      response shouldBe CompleteUploadResponse.BadRequest(s"JobId was not a UUID: $badJobId")
    }

    "successfully mark a job associated with an Upload payload as NotProcessing and not schedule it" in {

      val (jobScheduler, eventualEvent) = runScheduler()

      val client = createClient(createRoutes(jobScheduler))

      val jobId = JobId(createJobId())

      val createdJob = client
        .create(organizationId, jobId.value.toString, uploadPayload, authToken)
        .awaitFinite()
        .value
        .getOrFail[CreateResponse.Created, SwaggerJob]

      createdJob.state shouldBe Uploading

      val response = client
        .completeUpload(
          organizationId,
          jobId.value.toString,
          UploadResult(isSuccess = true),
          Nil :+ Authorization(OAuth2BearerToken(token.value))
        )
        .awaitFinite()
        .value

      response shouldBe CompleteUploadResponse.OK

      val job: Job = ports.db.run(get(jobId)).awaitFinite().get

      job.state shouldBe NotProcessing

      an[TimeoutException] should be thrownBy eventualEvent.awaitFinite()
    }

    "error when job is not associated with an upload payload" in {

      val (jobScheduler, _) = schedulerAndRunnableEvent()

      val client = createClient(createRoutes(jobScheduler))

      val jobId = JobId(createJobId())

      val createdJob = client
        .create(organizationId, jobId.value.toString, importPayload(), authToken)
        .awaitFinite()
        .value
        .getOrFail[CreateResponse.Created, SwaggerJob]

      ports.db
        .run(
          JobsMapper
            .updateState(jobId, Uploading)
        )
        .awaitFinite()

      val response = client
        .completeUpload(
          organizationId,
          jobId.value.toString,
          UploadResult(isSuccess = true),
          Nil :+ Authorization(OAuth2BearerToken(token.value))
        )
        .awaitFinite()
        .value

      response shouldBe CompleteUploadResponse.BadRequest(
        s"Job: ${jobId.value} not associated with an Upload payload. Actually: com.pennsieve.models.ETLWorkflow"
      )
    }

    "error when job is in an invalid state" in {
      val (jobScheduler, _) = schedulerAndRunnableEvent()

      val client = createClient(createRoutes(jobScheduler))

      val jobId = JobId(createJobId())

      val createdJob = client
        .create(organizationId, jobId.value.toString, uploadPayload, authToken)
        .awaitFinite()
        .value
        .getOrFail[CreateResponse.Created, SwaggerJob]

      ports.db
        .run(
          JobsMapper
            .updateState(jobId, Available)
        )
        .awaitFinite()

      val response = client
        .completeUpload(
          organizationId,
          jobId.value.toString,
          UploadResult(isSuccess = true),
          Nil :+ Authorization(OAuth2BearerToken(token.value))
        )
        .awaitFinite()
        .value

      response shouldBe CompleteUploadResponse.BadRequest(
        s"Job: ${jobId.value} is in an invalid state Available for the current operation."
      )
    }

    "jobs already in a Failed state return Accepted" in {

      val (jobScheduler, _) = schedulerAndRunnableEvent()

      val client = createClient(createRoutes(jobScheduler))

      val jobId = JobId(createJobId())

      val createdJob = client
        .create(organizationId, jobId.value.toString, uploadPayload, authToken)
        .awaitFinite()
        .value
        .getOrFail[CreateResponse.Created, SwaggerJob]

      ports.db
        .run(
          JobsMapper
            .updateState(jobId, Failed)
        )
        .awaitFinite()

      val response = client
        .completeUpload(
          organizationId,
          jobId.value.toString,
          UploadResult(isSuccess = true),
          Nil :+ Authorization(OAuth2BearerToken(token.value))
        )
        .awaitFinite()
        .value

      response shouldBe CompleteUploadResponse.Accepted

    }

    "jobs already marked as NotProcessing return Accepted" in {
      val (jobScheduler, _) = schedulerAndRunnableEvent()

      val client = createClient(createRoutes(jobScheduler))

      val jobId = JobId(createJobId())

      client
        .create(organizationId, jobId.value.toString, uploadPayload, authToken)
        .awaitFinite()
        .value
        .getOrFail[CreateResponse.Created, SwaggerJob]

      ports.db
        .run(
          JobsMapper
            .updateState(jobId, NotProcessing)
        )
        .awaitFinite()

      val response = client
        .completeUpload(
          organizationId,
          jobId.value.toString,
          UploadResult(isSuccess = true),
          Nil :+ Authorization(OAuth2BearerToken(token.value))
        )
        .awaitFinite()
        .value

      response shouldBe CompleteUploadResponse.Accepted
    }

    "fail an upload successfully" in {

      val (jobScheduler, _) = schedulerAndRunnableEvent()

      val client = createClient(createRoutes(jobScheduler))

      val jobId = JobId(createJobId())

      val createdJob = client
        .create(organizationId, jobId.value.toString, uploadPayload, authToken)
        .awaitFinite()
        .value
        .getOrFail[CreateResponse.Created, SwaggerJob]

      createdJob.state shouldBe Uploading

      val okUploadCompleteResponse = client
        .completeUpload(
          organizationId,
          jobId.value.toString,
          UploadResult(isSuccess = false),
          Nil :+ Authorization(OAuth2BearerToken(token.value))
        )
        .awaitFinite()
        .value

      okUploadCompleteResponse shouldBe CompleteUploadResponse.OK

      ports.db.run(JobsMapper.get(jobId)).awaitFinite().value.state shouldBe Failed
    }

    "set job state to cancelled for deleted package" in {
      val (jobScheduler, _) = schedulerAndRunnableEvent()

      val missingPackageSetUploadComplete: SetUploadComplete =
        (_, _, _, _, _) =>
          EitherT(
            Future.successful[Either[Throwable, UploadCompleteResponse]](
              UploadCompleteResponse.NotFound.asRight
            )
          )

      val client = createClient(
        createRoutes(
          jobScheduler,
          apiClient =
            fakePennsieveApiClient(injectedSetUploadComplete = missingPackageSetUploadComplete)
        )
      )

      val insertedJobId = JobId(createJobId())

      client
        .create(organizationId, insertedJobId.toString, uploadPayload, authToken)
        .awaitFinite()

      val response =
        client
          .completeUpload(
            organizationId,
            insertedJobId.toString,
            UploadResult(true),
            Nil :+ Authorization(OAuth2BearerToken(token.value))
          )
          .awaitFinite()
          .value

      response shouldBe CompleteUploadResponse.OK

      // the job's state should be updated in the database
      getJob(insertedJobId).state shouldBe Cancelled
    }

    "a failed upload should update the package state in API" in {
      var maybeUpdatePackageState = Option.empty[PackageState]

      val savingUpdatePackageState: SetPackageState =
        (_, _, _, state) => {
          maybeUpdatePackageState = Some(state)
          EitherT(Future.successful("".asRight[(StatusCode, Throwable)]))
        }

      val (jobScheduler, _) = runScheduler()

      val client =
        createClient(
          createRoutes(jobScheduler, apiClient = fakePennsieveApiClient(savingUpdatePackageState))
        )

      val jobId = JobId(createJobId())

      client
        .create(organizationId, jobId.value.toString, uploadPayload, authToken)
        .awaitFinite()
        .value
        .getOrFail[CreateResponse.Created, SwaggerJob]

      client
        .completeUpload(
          organizationId,
          jobId.value.toString,
          UploadResult(isSuccess = false),
          Nil :+ Authorization(OAuth2BearerToken(token.value))
        )
        .awaitFinite()
        .value

      maybeUpdatePackageState shouldBe Some(PackageState.UPLOAD_FAILED)
    }
  }

  "GET /organizations/:organizationId/jobs" should {
    "return all the jobs for that organization" in {
      val availableJob =
        insertJobInDB(organizationId, state = Available).toSwaggerJob
      val succeededJob =
        insertJobInDB(organizationId, state = Succeeded).toSwaggerJob

      val (jobScheduler, _) = schedulerAndRunnableEvent()
      val jobs: IndexedSeq[SwaggerJob] =
        createClient(createRoutes(jobScheduler))
          .getAllJobs(organizationId, List(Authorization(OAuth2BearerToken(token.value))))
          .value
          .awaitFinite()
          .value
          .getOrFail[GetAllJobsResponse.OK, IndexedSeq[SwaggerJob]]

      jobs.map(_.id) should contain allOf (availableJob.id, succeededJob.id)
    }

    "return forbidden for a token without permissions to access organization" in {
      val (jobScheduler, _) = schedulerAndRunnableEvent()

      val response =
        createClient(createRoutes(jobScheduler))
          .getAllJobs(forbiddenOrganization, List(Authorization(OAuth2BearerToken(token.value))))
          .value
          .awaitFinite()
          .value

      response shouldBe GetAllJobsResponse.Forbidden
    }

    "return not found for an organization that doesn't exist in the database" in {
      val notFoundOrganization: Int = organizationId + 2

      val (jobScheduler, _) = schedulerAndRunnableEvent()

      val notFoundClaim =
        generateClaim(UserId(userId), DatasetId(datasetId), OrganizationId(notFoundOrganization))

      val notFoundToken = Jwt.generateToken(notFoundClaim)(ports.jwt).value

      val response =
        createClient(createRoutes(jobScheduler))
          .getAllJobs(notFoundOrganization, List(Authorization(OAuth2BearerToken(notFoundToken))))
          .value
          .awaitFinite()
          .value

      response shouldBe GetAllJobsResponse.NotFound
    }
  }

  "GET /organizations/:organizationId/datasets/:datasetId/packages/:packageId/state" should {

    def getPackageFromResponse(
      resp: EitherT[Future, Either[Throwable, HttpResponse], GetPackageStateResponse]
    ): PackageState = {
      resp.value
        .awaitFinite()
        .value
        .getOrFail[GetPackageStateResponse.OK, io.circe.Json]
        .as[PackageState]
        .value
    }

    "get the state of a package based on its jobs simple" in {
      val (jobScheduler, _) = schedulerAndRunnableEvent()

      val job1 = insertAvailableJobInDB(
        1,
        payload = Upload(
          packageId = 1,
          datasetId = 1,
          userId = 1,
          encryptionKey = encryptionKey,
          files = List(s"${uploadDirectory}test.csv"),
          size = fileSize
        )
      )
      val job2 = insertAvailableJobInDB(1, payload = importPayload(1))
      val job3 = insertAvailableJobInDB(
        1,
        payload = ETLAppendWorkflow(
          packageId = 1,
          datasetId = 1,
          userId = 1,
          encryptionKey = encryptionKey,
          files = List(s"${uploadDirectory}test.csv"),
          assetDirectory = storageDirectory,
          fileType = FileType.CSV,
          packageType = PackageType.Tabular,
          channels = List.empty[Channel]
        )
      )
      //we turn job1 to succeeded
      ports.db.run(JobsMapper.updateState(job1.id, Succeeded)).awaitFinite()
      //we turn job2 to succeeded
      ports.db.run(JobsMapper.updateState(job2.id, Succeeded)).awaitFinite()
      //we turn job3 to failed
      ports.db.run(JobsMapper.updateState(job3.id, Failed)).awaitFinite()

      val packageStateResp =
        createClient(createRoutes(jobScheduler))
          .getPackageState(1, 1, 1, List(Authorization(OAuth2BearerToken(token.value))))

      val packageState = getPackageFromResponse(packageStateResp)

      packageState shouldBe PackageState.READY
    }

    "get the state of a package based on its jobs with no follow up append" in {
      val (jobScheduler, _) = schedulerAndRunnableEvent()

      val job1 = insertAvailableJobInDB(
        1,
        payload = Upload(
          packageId = 1,
          datasetId = 1,
          userId = 1,
          encryptionKey = encryptionKey,
          files = List(s"${uploadDirectory}test.csv"),
          size = fileSize
        )
      )
      val job2 = insertAvailableJobInDB(1, payload = importPayload(1))

      //we turn job1 to succeeded
      ports.db.run(JobsMapper.updateState(job1.id, Succeeded)).awaitFinite()
      //we turn job2 to succeeded
      ports.db.run(JobsMapper.updateState(job2.id, Succeeded)).awaitFinite()

      val packageStateResp =
        createClient(createRoutes(jobScheduler))
          .getPackageState(1, 1, 1, List(Authorization(OAuth2BearerToken(token.value))))

      val packageState = getPackageFromResponse(packageStateResp)

      packageState shouldBe PackageState.READY
    }

    "get the state of a package based on its jobs with only an upload job" in {
      val (jobScheduler, _) = schedulerAndRunnableEvent()

      val job1 = insertAvailableJobInDB(
        1,
        payload = Upload(
          packageId = 1,
          datasetId = 1,
          userId = 1,
          encryptionKey = encryptionKey,
          files = List(s"${uploadDirectory}test.csv"),
          size = fileSize
        )
      )

      //we turn job1 to succeeded
      ports.db.run(JobsMapper.updateState(job1.id, Succeeded)).awaitFinite()

      val packageStateResp =
        createClient(createRoutes(jobScheduler))
          .getPackageState(1, 1, 1, List(Authorization(OAuth2BearerToken(token.value))))

      val packageState = getPackageFromResponse(packageStateResp)

      packageState shouldBe PackageState.READY
    }

    "get the state of a package based on its jobs with only a job in non-terminal state" in {
      val (jobScheduler, _) = schedulerAndRunnableEvent()

      val job1 = insertAvailableJobInDB(
        1,
        payload = Upload(
          packageId = 1,
          datasetId = 1,
          userId = 1,
          encryptionKey = encryptionKey,
          files = List(s"${uploadDirectory}test.csv"),
          size = fileSize
        )
      )

      //we turn job1 to succeeded
      ports.db.run(JobsMapper.updateState(job1.id, Uploading)).awaitFinite()

      val packageStateResp =
        createClient(createRoutes(jobScheduler))
          .getPackageState(1, 1, 1, List(Authorization(OAuth2BearerToken(token.value))))

      val packageState = getPackageFromResponse(packageStateResp)

      packageState shouldBe PackageState.UNAVAILABLE
    }

    "not get the state of a package based on its jobs when it has no job" in {
      val (jobScheduler, _) = schedulerAndRunnableEvent()

      val packageState =
        createClient(createRoutes(jobScheduler))
          .getPackageState(1, 1, 1, List(Authorization(OAuth2BearerToken(token.value))))
          .value
          .awaitFinite()
          .value

      packageState shouldBe GetPackageStateResponse.NotFound
    }

    "not get the state of a package based on its jobs when user is not authorized on dataset" in {
      val (jobScheduler, _) = schedulerAndRunnableEvent()

      val newClaim: Jwt.Claim = generateClaim(UserId(1), DatasetId(2), OrganizationId(1))
      val newToken: Jwt.Token = Jwt.generateToken(newClaim)(ports.jwt)
      val newAuthToken = List(Authorization(OAuth2BearerToken(newToken.value)))

      val packageState =
        createClient(createRoutes(jobScheduler))
          .getPackageState(1, 1, 1, newAuthToken)
          .value
          .awaitFinite()
          .value

      packageState shouldBe GetPackageStateResponse.Forbidden
    }

    "not get the state of a package based on its jobs when the package does not belong to the dataset" in {
      val (jobScheduler, _) = schedulerAndRunnableEvent()

      val job1 = insertAvailableJobInDB(
        1,
        payload = Upload(
          packageId = 1,
          datasetId = 2,
          userId = 1,
          encryptionKey = encryptionKey,
          files = List(s"${uploadDirectory}test.csv"),
          size = fileSize
        )
      )

      //we turn job1 to succeeded
      ports.db.run(JobsMapper.updateState(job1.id, Succeeded)).awaitFinite()

      val packageState =
        createClient(createRoutes(jobScheduler))
          .getPackageState(1, 1, 1, List(Authorization(OAuth2BearerToken(token.value))))
          .value
          .awaitFinite()
          .value

      packageState shouldBe GetPackageStateResponse.NotFound
    }

  }

  "GET /organizations/:organizationId/jobs/:jobId" should {
    "return a job for valid request" in {
      val (jobScheduler, _) = schedulerAndRunnableEvent()

      val expectedJob =
        insertJobInDB(organizationId, state = Available).toSwaggerJob

      val job =
        createClient(createRoutes(jobScheduler))
          .getJob(
            organizationId,
            expectedJob.id.toString,
            List(Authorization(OAuth2BearerToken(token.value)))
          )
          .value
          .awaitFinite()
          .value
          .getOrFail[GetJobResponse.OK, SwaggerJob]

      job.id shouldBe expectedJob.id
    }

    "return forbidden for a token without permissions to access organization" in {
      val (jobScheduler, _) = schedulerAndRunnableEvent()

      val response =
        createClient(createRoutes(jobScheduler))
          .getJob(
            forbiddenOrganization,
            UUID.randomUUID().toString,
            List(Authorization(OAuth2BearerToken(token.value)))
          )
          .value
          .awaitFinite()
          .value

      response shouldBe GetJobResponse.Forbidden
    }

    "return not found for an organization that doesn't exist in the database" in {
      val notFoundOrganization: Int = organizationId + 2

      val (jobScheduler, _) = schedulerAndRunnableEvent()

      val notFoundClaim =
        generateClaim(UserId(userId), DatasetId(datasetId), OrganizationId(notFoundOrganization))

      val notFoundToken = Jwt.generateToken(notFoundClaim)(ports.jwt).value

      val response =
        createClient(createRoutes(jobScheduler))
          .getJob(
            notFoundOrganization,
            UUID.randomUUID().toString,
            List(Authorization(OAuth2BearerToken(notFoundToken)))
          )
          .value
          .awaitFinite()
          .value

      response shouldBe GetJobResponse.NotFound("No organization found for 3")
    }
  }

  "GET /organizations/:organizationId/packages/:packageId/jobs" should {
    "return unauthorized for a unauthorized request" in {
      val (jobScheduler, _) = schedulerAndRunnableEvent()

      val response =
        createClient(createRoutes(jobScheduler))
          .getPackageJobs(
            forbiddenOrganization,
            packageId,
            headers = List(Authorization(OAuth2BearerToken(token.value)))
          )
          .value
          .awaitFinite()
          .value

      response shouldBe GetPackageJobsResponse.Forbidden
    }

    "return a page of jobs for a package id" in {
      val (jobScheduler, _) = schedulerAndRunnableEvent()

      val insertedJob =
        insertJobInDB(
          organizationId,
          state = Available,
          payload = Some(uploadPayload.copy(packageId = packageId))
        )

      val actualPackageId =
        (getJobWithPayload(ports.db) _)(insertedJob.id)
          .awaitFinite()
          .toOption
          .flatMap(_.flatMap(_._2.packageId))

      actualPackageId shouldBe Some(packageId)

      val jobPage =
        createClient(createRoutes(jobScheduler))
          .getPackageJobs(
            datasetId,
            packageId,
            headers = List(Authorization(OAuth2BearerToken(token.value)))
          )
          .value
          .awaitFinite()
          .value
          .getOrFail[GetPackageJobsResponse.OK, JobPage]

      jobPage shouldBe JobPage(Vector(insertedJob.toClientJob))
    }

    "return a page of jobs with a cursor if more than the expected page size are present" in {
      val (jobScheduler, _) = schedulerAndRunnableEvent()

      val oldJobRecord = createJob(packageId = Some(uploadPayload.packageId))
      val oldJob = insertETLJobInDB(oldJobRecord, createJobState(oldJobRecord.id, Uploading))

      val nextJob = insertJobInDB(organizationId, state = Uploading)

      val someCursor = Some(Cursor(nextJob.id, nextJob.createdAt).toString)
      val expectedPage = JobPage(Vector(oldJob.toClientJob), someCursor)

      val jobPage =
        createClient(createRoutes(jobScheduler))
          .getPackageJobs(
            datasetId,
            packageId,
            1,
            headers = List(Authorization(OAuth2BearerToken(token.value)))
          )
          .value
          .awaitFinite()
          .value
          .getOrFail[GetPackageJobsResponse.OK, JobPage]

      jobPage shouldBe expectedPage
    }

    "return the next page for a cursor" in {
      val (jobScheduler, _) = schedulerAndRunnableEvent()

      val oldJobRecord = createJob(packageId = Some(packageId))
      insertETLJobInDB(oldJobRecord, createJobState(oldJobRecord.id, Uploading))

      val nextJob = insertJobInDB(organizationId, state = Uploading)

      val expectedPage =
        JobPage(Vector(nextJob.toClientJob), None)

      val client = createClient(createRoutes(jobScheduler))

      val firstJobPage =
        client
          .getPackageJobs(
            datasetId,
            packageId,
            pageSize = 1,
            headers = List(Authorization(OAuth2BearerToken(token.value)))
          )
          .value
          .awaitFinite()
          .value
          .getOrFail[GetPackageJobsResponse.OK, JobPage]

      val nextJobPage =
        client
          .getPackageJobs(
            datasetId,
            packageId,
            pageSize = 1,
            cursor = firstJobPage.cursor,
            headers = List(Authorization(OAuth2BearerToken(token.value)))
          )
          .value
          .awaitFinite()
          .value
          .getOrFail[GetPackageJobsResponse.OK, JobPage]

      nextJobPage shouldBe expectedPage
    }
  }

  "JobStates inserted in to the database should map exactly to those displayed with the client" in {
    val insertedJob = insertJobInDB(organizationId, state = Uploading)
    val (jobScheduler, _) = schedulerAndRunnableEvent()

    val time = OffsetDateTime.now(UTC)

    JobState.values.foreach { state =>
      ports.db
        .run(JobsMapper.updateState(insertedJob.id, state, taskId, time.plusSeconds(1L)))
        .awaitFinite()

      val actualJob =
        createClient(createRoutes(jobScheduler))
          .getJob(
            organizationId,
            insertedJob.id.toString,
            headers = List(Authorization(OAuth2BearerToken(token.value)))
          )
          .value
          .awaitFinite()
          .value
          .getOrFail[GetJobResponse.OK, SwaggerJob]

      actualJob.state shouldBe state
    }
  }

  def getJob(jobId: JobId): Job = ports.db.run(JobsMapper.get(jobId)).awaitFinite().value

  implicit class RichServerSwaggerJob(job: Job) {
    import io.scalaland.chimney.dsl.TransformerOps
    def toClientJob: SwaggerJob =
      job.toSwaggerJob.transformInto[SwaggerJob]
  }

  def successfulNotify(sentNotifications: ArrayBuffer[MessageBody]): SendMessage = msg => {
    sentNotifications += msg
    Future.successful(Right(SendMessageResponse.builder().build()))
  }

  def createJobId(): UUID = UUID.randomUUID()

  def createJobIdString(): String = createJobId().toString

  // http://doc.akka.io/docs/akka-http/10.0.0/scala/http/routing-dsl/testkit.html#testing-sealed-routes
  def createRoutes(
    jobScheduler: JobScheduler,
    notifyUploadConsumer: NotifyUpload = successfulSendMessage,
    createJob: CreateJob = JobsHandlerPorts.createJob(ports.db),
    apiClient: PennsieveApiClient = fakePennsieveApiClient()
  ): Route =
    Route.seal(
      JobsHandler.routes(
        JobsHandlerPorts(
          createJob,
          notifyUploadConsumer,
          JobsHandlerPorts.getJobs(ports.db),
          JobsHandlerPorts.getOrganization(ports.db),
          JobSchedulingPorts.createGetJob(ports.db),
          JobsHandlerPorts.setJobState(ports.db),
          JobsHandlerPorts.getLastJobNotAppend(ports.db),
          JobsHandlerPorts.getJobWithPayload(ports.db),
          JobsHandlerPorts.packageUploadComplete(apiClient, ports.db),
          JobsHandlerPorts.getJobsForPackage(ports.db),
          JobsHandlerPorts.updatePackageState(apiClient, ports.db),
          ports.jwt
        ),
        jobScheduler
      )
    )

  def createClient(routes: Route): JobsClient = JobsClient.httpClient(Route.toFunction(routes))

  def generateClaim(
    userId: UserId,
    datasetId: DatasetId,
    organizationId: OrganizationId,
    duration: FiniteDuration = FiniteDuration(1, MINUTES),
    organizationRole: Role = Role.Owner,
    datasetRole: Role = Role.Editor
  ): Jwt.Claim =
    Jwt.generateClaim(
      duration = duration,
      content = UserClaim(
        id = userId,
        roles = List(
          Jwt.OrganizationRole(
            id = organizationId.inject[RoleIdentifier[OrganizationId]],
            role = organizationRole
          ),
          Jwt.DatasetRole(id = datasetId.inject[RoleIdentifier[DatasetId]], role = datasetRole)
        )
      )
    )
}
