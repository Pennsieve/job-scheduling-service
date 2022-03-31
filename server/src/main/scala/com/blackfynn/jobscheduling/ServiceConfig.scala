// Copyright (c) [2018] - [2022] Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.jobscheduling

import com.amazonaws.regions.Regions
import com.pennsieve.jobscheduling.clients.SQSClient.QueueName

import scala.concurrent.duration._
import scala.concurrent.duration.FiniteDuration

case class JobMonitorConfig(
  region: String,
  queue: String,
  parallelism: Int,
  throttle: ThrottleConfig,
  retry: RetryConfig
) {
  val awsRegion: Regions = Regions.fromName(region)
  val queueName: QueueName = QueueName(queue)
}

case class ServiceConfig(
  jobMonitor: JobMonitorConfig,
  ecs: ECSConfig,
  pennsieveApi: PennsieveApiConfig,
  jobScheduler: JobSchedulerConfig,
  jwt: JwtConfig,
  postgres: PostgresConfig,
  pusher: PusherConfig,
  s3: S3Config,
  watchDog: WatchDogConfig,
  uploadsConsumer: SQSConfig,
  notifications: SQSConfig,
  host: String = "0.0.0.0",
  port: Int = 8080
)

case class ECSConfig(
  region: String,
  cluster: String,
  rawSubnetIds: String,
  rawSecurityGroups: String,
  nextflow: NextflowConfig,
  task: ECSTaskConfig
) {
  val awsRegion: Regions = Regions.fromName(region)
  val subnetIds: List[String] = rawSubnetIds.split(",").toList
  val securityGroups: List[String] = rawSecurityGroups.split(",").toList
}

case class ECSTaskConfig(
  containerCPU: Int,
  iamRole: String,
  containerMemory: Int,
  maxAttempts: Int,
  taskDefinition: String
)

case class PennsieveApiConfig(baseUrl: String, queueSize: Int, rateLimit: Int)

case class SQSConfig(queue: String, region: String) {
  val awsRegion: Regions = Regions.fromName(region)
  val queueName: QueueName = QueueName(queue)
}

case class JobSchedulerConfig(
  maxTasks: Int,
  bufferSize: Int,
  throttle: ThrottleConfig,
  retry: RetryConfig
)

case class JwtConfig(key: String, duration: FiniteDuration = 5.minutes)

case class NextflowConfig(accessKeyId: String, accessKeySecret: String)

case class PostgresConfig(
  host: String,
  port: Int,
  database: String,
  user: String,
  password: String,
  maxConnections: Int = 100,
  minThreads: Int = 100,
  maxThreads: Int = 100,
  queueSize: Int = 1000,
  useSSL: Boolean = true
) {
  final val driver: String = "org.postgresql.Driver"

  private val jdbcBaseURL: String = s"jdbc:postgresql://$host:$port/$database"
  final val jdbcURL = {
    if (useSSL) jdbcBaseURL + "?ssl=true&sslmode=verify-ca"
    else jdbcBaseURL
  }
}

case class PusherConfig(environment: String)

case class S3Config(host: String, region: String, etlBucket: String) {
  val awsRegion: Regions = Regions.fromName(region)
}

case class ThrottleConfig(parallelism: Int, period: FiniteDuration = 1 second)

case class RetryConfig(
  maxRetries: Int,
  delay: FiniteDuration = 1 second,
  resetAfter: FiniteDuration = 5 minutes
)

case class TimerConfig(startAfter: FiniteDuration, runEvery: FiniteDuration)

case class JobWatchDogConfig(ecs: ThrottleConfig, sink: ThrottleConfig)

case class JobStateWatchDogConfig(minutesStuck: Long, maxRetries: Int, throttle: ThrottleConfig)

case class WatchDogConfig(
  hoursRunning: Long,
  noJobThrottle: ThrottleConfig,
  jobThrottle: JobWatchDogConfig,
  jobState: JobStateWatchDogConfig,
  timer: TimerConfig,
  retry: RetryConfig
)

case class JobsHandlerConfig(uploadsConsumer: SQSConfig, notifications: SQSConfig)
