// Copyright (c) [2018] - [2025] Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.jobscheduling

import com.pennsieve.jobscheduling.TestConfig._
import com.pennsieve.service.utilities.ContextLogger
import com.pennsieve.test.AwaitableImplicits
import com.spotify.docker.client.DefaultDockerClient
import com.spotify.docker.client.exceptions.DockerException
import com.whisk.docker.DockerFactory
import com.whisk.docker.impl.spotify.SpotifyDockerFactory
import com.whisk.docker.scalatest.DockerTestKit
import org.scalatest.time.{ Second, Seconds, Span }
import org.scalatest.{ BeforeAndAfterAll, OptionValues, Suite }

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import scala.util.{ Failure, Success }

trait JobSchedulingServiceSpecHarness
    extends Suite
    with BeforeAndAfterAll
    with DockerPostgresService
    with DockerTestKit
    with AwaitableImplicits
    with OptionValues { suite: Suite =>

  // increase default patience to allow containers to come up
  implicit val patience: PatienceConfig =
    PatienceConfig(Span(30, Seconds), Span(1, Second))

  // provide a dockerFactory
  override implicit val dockerFactory: DockerFactory =
    try new SpotifyDockerFactory(
      DefaultDockerClient
        .fromEnv()
        .apiVersion("v1.41")
        .build()
    )
    catch {
      case _: DockerException => fail(new DockerException("Docker may not be running"))
    }

  implicit val log: ContextLogger = new ContextLogger

  val etlConfig: ServiceConfig = ServiceConfig(
    jobMonitor = staticJobMonitorConfig,
    ecs = staticEcsConfig,
    pennsieveApi = staticPennsieveApiConfig,
    jobScheduler = staticJobSchedulerConfig,
    jwt = staticJwtConfig,
    postgres = postgresConfiguration,
    pusher = staticPusherConfig,
    s3 = staticS3Config,
    watchDog = staticWatchDogConfig,
    uploadsConsumer = SQSConfig("queue", "us-east-1")
  )

  lazy implicit val ports: JobSchedulingPorts = new JobSchedulingPorts(etlConfig)

  override def beforeAll(): Unit = {
    super.beforeAll()

    val setup =
      isContainerReady(postgresContainer)
        .map(_ => DatabaseMigrator.run(etlConfig.postgres, onFailRepair = true))

    Await.result(setup, 30.seconds) match {
      case Success(_) => ()
      case Failure(ex) => fail(ex)
    }
  }

  override def afterAll(): Unit = {
    ports.db.close()

    super.afterAll()
  }
}

trait UnhealthyDBJobSchedulingServiceSpecHarness extends JobSchedulingServiceSpecHarness {

  override def beforeAll(): Unit = {
    super.beforeAll()
    // Terminate the database connection prematurely:
    Await.result(ports.db.shutdown, 30.seconds)
  }
}
