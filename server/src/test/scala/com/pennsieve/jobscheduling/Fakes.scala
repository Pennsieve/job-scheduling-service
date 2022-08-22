// Copyright (c) [2018] - [2022] Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.jobscheduling

import akka.http.scaladsl.model.StatusCode
import akka.http.scaladsl.model.StatusCodes.NotFound
import cats.data.EitherT
import com.amazonaws.AmazonServiceException
import com.amazonaws.services.ecs.model._
import software.amazon.awssdk.services.sqs.model.{ DeleteMessageResponse, SendMessageResponse }
import com.pennsieve.auth.middleware.{ DatasetId, Jwt, OrganizationId, UserId }
import com.pennsieve.core.clients.packages.UploadCompleteResponse
import com.pennsieve.core.clients.packages.UploadCompleteResponse.OK
import com.pennsieve.jobscheduling.JobSchedulingPorts._
import com.pennsieve.jobscheduling.TestTask.{ createTask, runTaskResult }
import com.pennsieve.jobscheduling.clients.PennsieveApiClient
import com.pennsieve.jobscheduling.clients.SQSClient.{ ReceiptHandle, SendAck, SendMessage }
import com.pennsieve.jobscheduling.commons.JobState
import com.pennsieve.jobscheduling.db.TaskId
import com.pennsieve.jobscheduling.db.profile.api.Database
import com.pennsieve.jobscheduling.handlers.JobsHandlerPorts.NotifyUpload
import com.pennsieve.jobscheduling.model.EventualResult.{
  EventualResponseT,
  EventualResult,
  EventualResultT
}
import com.pennsieve.jobscheduling.model.PackageId
import com.pennsieve.jobscheduling.scheduler.JobQueued
import com.pennsieve.models.{ JobId, Manifest, PackageState }
import com.pennsieve.test.AwaitableImplicits

import java.time.OffsetDateTime
import scala.jdk.CollectionConverters._
import scala.concurrent.{ ExecutionContext, Future }

object Fakes extends AwaitableImplicits {
  import cats.implicits._

  val getManifest: GetManifest = _ => Future.successful(Right(TestPayload.uploadManifest))

  val notifyJobSource: NotifyJobSource = () => Future.successful(Right(JobQueued))

  val jwt: Jwt.Config = new Jwt.Config {
    val key: String = "I'm not a real jwt key, I just play one in a test suite"
  }

  def failingUpdateJob(
    db: Database,
    failingAttempts: Int
  )(implicit
    ec: ExecutionContext
  ): UpdateJob = {
    var count = 0
    (
      jobId: JobId,
      newState: JobState,
      sentAt: OffsetDateTime,
      taskId: Option[TaskId],
      log: ETLLogContext
    ) => {
      count += 1
      if (count <= failingAttempts) throw new Exception("This is just a test.")
      else createUpdateJob(db)(ec)(jobId, newState, sentAt, taskId, log)
    }
  }

  val successfulNotification: SendMessage = _ =>
    Future.successful(Right(SendMessageResponse.builder().build()))

  val receiptHandle: ReceiptHandle = ReceiptHandle("receiptHandle")

  val successfulAck: SendAck = _ =>
    Future.successful(Right(DeleteMessageResponse.builder().build()))

  def getPayloadReal(implicit ports: JobSchedulingPorts): GetPayload = createGetPayload(ports.db)

  type SetPackageState = (Int, Int, Int, PackageState) => EventualResponseT[String]
  type SetUploadComplete =
    (PackageId, DatasetId, OrganizationId, UserId, JobId) => EventualResultT[UploadCompleteResponse]

  val successfulSetPackageState: SetPackageState =
    (_, _, _, _) => EitherT(Future.successful("".asRight[(StatusCode, Throwable)]))

  val successfulSetUploadComplete: SetUploadComplete =
    (_, _, _, _, _) =>
      EitherT(Future.successful[Either[Throwable, UploadCompleteResponse]](OK.asRight[Throwable]))

  val missingPackageSetPackageState: SetPackageState =
    (_, _, _, _) =>
      EitherT[Future, (StatusCode, Throwable), String](
        Future.successful(Left((NotFound, new Exception(""))))
      )

  val notImplementedSetPackageState: SetPackageState =
    (_, _, _, _) => ???

  def fakePennsieveApiClient(
    injectedSetPackageState: SetPackageState = successfulSetPackageState,
    injectedSetUploadComplete: SetUploadComplete = successfulSetUploadComplete
  ): PennsieveApiClient =
    new PennsieveApiClient {
      override def setPackageState(
        organizationId: Int,
        datasetId: Int,
        packageId: Int,
        state: PackageState
      ): EventualResponseT[String] =
        injectedSetPackageState(organizationId, datasetId, packageId, state)

      override def setPackageUploadComplete(
        packageId: PackageId,
        datasetId: DatasetId,
        organizationId: OrganizationId,
        userId: UserId,
        jobId: JobId
      )(implicit
        ec: ExecutionContext
      ): EventualResultT[UploadCompleteResponse] =
        injectedSetUploadComplete(packageId, datasetId, organizationId, userId, jobId)
    }
}

class FakeSQS() {
  import io.circe.parser.decode

  var sentManifest: Option[Manifest] = None

  val savingManifestUploadNotifier: NotifyUpload =
    string => {
      sentManifest = Some(decode[Manifest](string.value).toOption.get)
      Future.successful(Right(SendMessageResponse.builder().build()))
    }
}

class FakeFargate(attemptsTilSuccess: Int = 1) {

  private var listAttempts = 0

  var taskAttempts = 0

  var stopAttempts = 0

  var successfulRuns = 0

  var describeAttempts = 0

  var stoppedTask: Option[String] = None

  def getSuccessfulRuns: Int = successfulRuns

  def listTasks(request: ListTasksRequest): EventualResult[ListTasksResult] =
    Future.successful {
      val listTasksResult = new ListTasksResult()
      if (listAttempts < attemptsTilSuccess)
        listTasksResult.setTaskArns(List("task").asJava)
      listAttempts += 1
      Right(listTasksResult)
    }

  def throwingListTasks(request: ListTasksRequest): EventualResult[ListTasksResult] =
    if (taskAttempts < attemptsTilSuccess) {
      taskAttempts += 1
      Future.successful(Left(new AmazonServiceException("List task failed")))
    } else Future.successful(Right(new ListTasksResult()))

  def runTask(task: Task = createTask())(request: RunTaskRequest): EventualResult[RunTaskResult] =
    if (taskAttempts < attemptsTilSuccess) {
      taskAttempts += 1
      Future.successful(Left(new AmazonServiceException("Task failed")))
    } else {
      taskAttempts += 1
      successfulRuns += 1
      Future.successful(Right(runTaskResult(task)))
    }

  def stopTask(request: StopTaskRequest): EventualResult[StopTaskResult] =
    if (stopAttempts < attemptsTilSuccess) {
      stopAttempts += 1
      Future.successful(Left(new AmazonServiceException("Failed to stop")))
    } else {
      val task = new Task().withTaskArn(request.getTask)
      stoppedTask = Some(task.getTaskArn)
      Future.successful(Right(new StopTaskResult().withTask(task)))
    }

  def failingStopTask(request: StopTaskRequest): EventualResult[StopTaskResult] =
    if (stopAttempts < attemptsTilSuccess) {
      stopAttempts += 1
      throw new Exception("This is just a test.")
    } else {
      val task = new Task().withTaskArn(request.getTask)
      stoppedTask = Some(task.getTaskArn)
      Future.successful(Right(new StopTaskResult().withTask(task)))
    }

  def describeTasks(
    task: Task
  )(
    request: DescribeTasksRequest
  ): EventualResult[DescribeTasksResult] =
    if (describeAttempts < attemptsTilSuccess) {
      describeAttempts += 1
      Future.successful(Left(new AmazonServiceException("Can't get description")))
    } else {
      Future.successful(Right(new DescribeTasksResult().withTasks(task)))
    }

  def successfulDescribeTasks(
    task: Task
  )(
    request: DescribeTasksRequest
  ): EventualResult[DescribeTasksResult] =
    Future.successful(Right(new DescribeTasksResult().withTasks(task)))
}
