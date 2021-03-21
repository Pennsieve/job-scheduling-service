// Copyright (c) [2018] - [2020] Blackfynn, Inc. All Rights Reserved.

package com.blackfynn.jobscheduling

import java.net.ServerSocket
import java.nio.file.Paths
import java.time.OffsetDateTime
import java.time.ZoneOffset.UTC
import java.util.UUID

import cats.syntax.option.catsSyntaxOptionId
import com.amazonaws.services.ecs.model.{ RunTaskResult, Task }
import com.blackfynn.auth.middleware.{ OrganizationId, UserId }
import com.blackfynn.jobscheduling.commons.JobState
import com.blackfynn.jobscheduling.commons.JobState.Running
import com.blackfynn.jobscheduling.db.PayloadsMapper.create
import com.blackfynn.jobscheduling.db._
import com.blackfynn.models._
import com.blackfynn.test.AwaitableImplicits
import pureconfig.loadConfigOrThrow

import scala.concurrent.ExecutionContext

object TestPostgresConfiguration {
  val advertisedPort: Int = 5432

  def freshPostgresConfiguration: PostgresConfig = {
    val exposedPort: Int = {
      // ServerSocket will find an available port given port "0"
      val socket = new ServerSocket(0)
      val port = socket.getLocalPort
      socket.close()
      port
    }

    PostgresConfig(
      host = "localhost",
      port = exposedPort,
      database = "postgres",
      user = "postgres",
      password = "password",
      useSSL = false
    )
  }
}

object TestConfig {

  case class RootConfig(
    jobMonitor: JobMonitorConfig,
    ecs: ECSConfig,
    pennsieveApi: PennsieveApiConfig,
    jobScheduler: JobSchedulerConfig,
    notifications: SQSConfig,
    jwt: JwtConfig,
    pusher: PusherConfig,
    s3: S3Config,
    watchDog: WatchDogConfig
  )

  private val testConfPath =
    Paths.get(getClass.getResource("/application-test.conf").getPath)

  private lazy val rootConfig: RootConfig = loadConfigOrThrow[RootConfig](testConfPath)

  lazy val staticJobMonitorConfig: JobMonitorConfig = rootConfig.jobMonitor

  lazy val staticNotificationsConfig: SQSConfig = rootConfig.notifications

  lazy val staticEcsConfig: ECSConfig = rootConfig.ecs

  lazy val staticPennsieveApiConfig: PennsieveApiConfig = rootConfig.pennsieveApi

  lazy val staticJobSchedulerConfig: JobSchedulerConfig = rootConfig.jobScheduler

  lazy val staticJwtConfig: JwtConfig = rootConfig.jwt

  lazy val staticPusherConfig: PusherConfig = rootConfig.pusher

  lazy val staticS3Config: S3Config = rootConfig.s3

  lazy val staticWatchDogConfig: WatchDogConfig = rootConfig.watchDog
}

object TestTask {
  val taskArn = "task-arn"
  val clusterArn = "cluster-arn"

  def createTask(): Task = {
    new Task()
      .withTaskArn(taskArn)
      .withClusterArn(clusterArn)
  }

  def runTaskResult(task: Task = createTask()): RunTaskResult = new RunTaskResult().withTasks(task)

  val taskId = TaskId(taskArn, clusterArn)
}

object TestPayload extends AwaitableImplicits {
  val userId: Int = 1
  val organizationId: Int = 1

  val packageId: Int = 1
  val sourcePackageId: Int = 2
  val datasetId: Int = 1

  val defaultJobId: JobId = JobId(UUID.randomUUID)
  val packageNodeId: String = s"N:package:$defaultJobId"
  val uploadDirectory: String = s"test@pennsieve.com/$defaultJobId/"
  val storageDirectory: String =
    s"test@pennsieve.com/data/$defaultJobId/"
  val encryptionKey: String = "test-encryption-key"

  val fileSize: Long = 100L

  val uploadPayload = Upload(
    packageId = packageId,
    datasetId = datasetId,
    userId = userId,
    encryptionKey = encryptionKey,
    files = List(s"${uploadDirectory}test.csv"),
    size = fileSize
  )

  val uploadManifest = Manifest(PayloadType.Upload, defaultJobId, organizationId, uploadPayload)

  val yesterday: OffsetDateTime = OffsetDateTime.now(UTC).minusDays(1L)

  val fifteenMinutesAgo: OffsetDateTime = OffsetDateTime.now(UTC).minusMinutes(15L)

  val exportPayload = ETLExportWorkflow(
    packageId = packageId,
    datasetId = datasetId,
    userId = userId,
    encryptionKey = encryptionKey,
    packageType = PackageType.TimeSeries,
    fileType = FileType.NeuroDataWithoutBorders,
    sourcePackageId = sourcePackageId,
    sourcePackageType = PackageType.TimeSeries
  )

  val appendPayload = ETLAppendWorkflow(
    packageId = packageId,
    datasetId = datasetId,
    userId = userId,
    encryptionKey = encryptionKey,
    files = List(s"${uploadDirectory}test.csv"),
    assetDirectory = storageDirectory,
    fileType = FileType.CSV,
    packageType = PackageType.Tabular,
    channels = List.empty[Channel]
  )

  def importPayload(packageId: Int = 1, userId: Int = 1) = ETLWorkflow(
    packageId = packageId,
    datasetId = datasetId,
    userId = userId,
    encryptionKey = encryptionKey,
    files = List(s"${uploadDirectory}test.csv"),
    assetDirectory = storageDirectory,
    fileType = FileType.CSV,
    packageType = PackageType.Tabular
  )

  def insertETLJobInDB(
    job: JobRecord,
    state: JobStateRecord,
    payload: Payload = importPayload()
  )(implicit
    ports: JobSchedulingPorts,
    ec: ExecutionContext
  ): Job =
    ports.db
      .run {
        JobsMapper.update(job, state, payload)
      }
      .awaitFinite()

  def insertJobInDB(
    orgId: Int,
    userId: Int = 3,
    state: JobState,
    jobId: JobId = JobId(UUID.randomUUID()),
    payload: Option[Payload] = None
  )(implicit
    ports: JobSchedulingPorts,
    ec: ExecutionContext
  ): Job = {
    ports.db
      .run(
        JobsMapper
          .createWithPayload(
            jobId,
            payload.getOrElse(uploadPayload.copy(userId = userId)),
            OrganizationId(orgId),
            state = state,
            Some(UserId(userId))
          )
      )
      .awaitFinite()
  }

  def createJob(
    maybeTaskId: Option[TaskId] = None,
    time: OffsetDateTime = yesterday,
    packageId: Option[Int] = None
  )(implicit
    ports: JobSchedulingPorts
  ): JobRecord =
    JobRecord(
      id = JobId(UUID.randomUUID()),
      payloadId = ports.db.run(create(uploadPayload, packageId)).awaitFinite().id,
      organizationId = 1,
      userId = Some(1),
      completed = false,
      taskId = maybeTaskId,
      submittedAt = Some(time),
      createdAt = time,
      updatedAt = time
    )

  def createJobState(jobId: JobId, state: JobState = Running): JobStateRecord =
    JobStateRecord(jobId = jobId, state = state)

  def createOldJobState(id: JobId, state: JobState): JobStateRecord =
    JobStateRecord(id, sentAt = fifteenMinutesAgo, state = state)

  def entryToJob(entry: JobRecord, state: JobState = Running): Job =
    Job(
      entry.id,
      entry.payloadId,
      entry.organizationId,
      entry.userId,
      state,
      JobState.terminalStates.contains(state),
      entry.taskId,
      entry.submittedAt,
      entry.createdAt,
      entry.updatedAt
    )

  def insertAvailableJobInDB(
    orgId: Int,
    userId: Int = 3,
    jobId: JobId = JobId(UUID.randomUUID()),
    payload: Payload = importPayload()
  )(implicit
    ports: JobSchedulingPorts,
    ec: ExecutionContext
  ): Job = {
    insertJobInDB(orgId, userId, JobState.Available, jobId, payload.some)
  }
}
