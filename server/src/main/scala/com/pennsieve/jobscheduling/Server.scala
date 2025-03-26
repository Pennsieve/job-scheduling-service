// Copyright (c) [2018] - [2025] Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.jobscheduling

import java.net.URI
import akka.actor.{ ActorSystem, Scheduler }
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{ HttpRequest, HttpResponse }
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.pennsieve.core.clients.packages.PackagesClient
import com.pennsieve.jobscheduling.clients.{ HttpClient, PennsieveApiClientImpl }
import com.pennsieve.jobscheduling.handlers.{
  HealthcheckHandler,
  JobsHandler,
  JobsHandlerPorts,
  OrganizationsHandler
}
import com.pennsieve.jobscheduling.monitor.{ JobMonitor, JobMonitorPorts }
import com.pennsieve.jobscheduling.scheduler.{ JobScheduler, JobSchedulerPorts }
import com.pennsieve.jobscheduling.watchdog.{ WatchDog, WatchDogPorts }
import com.pennsieve.service.utilities.{ ContextLogger, MigrationRunner, QueueHttpResponder }
import pureconfig.ConfigSource
import pureconfig.generic.auto._

import scala.concurrent.{ ExecutionContext, Future }
import scala.util.{ Failure, Success, Try }

object DatabaseMigrator {

  def run(
    configuration: PostgresConfig,
    onFailRepair: Boolean
  )(implicit
    log: ContextLogger
  ): Try[Unit] = {
    Try {
      new MigrationRunner(
        configuration.jdbcURL,
        configuration.user,
        configuration.password,
        schema = Some(db.schema)
      )
    }.map { migrator =>
      Try(migrator.run()) match {
        case Success((count, _)) =>
          log.noContext.info(s"Successfully ran $count migrations on ${configuration.jdbcURL}")

        case Failure(err) =>
          migrator.migrator.repair()
          if (onFailRepair) run(configuration, onFailRepair = false)
          log.noContext.error(s"Failed to run migrations with ${err.getMessage}")
      }
    }
  }
}

object Server extends App {

  implicit val config: ServiceConfig = ConfigSource.default.loadOrThrow[ServiceConfig]

  implicit val system: ActorSystem = ActorSystem("job-scheduling-service")
  implicit val executionContext: ExecutionContext = system.dispatcher
  implicit val scheduler: Scheduler = system.scheduler
  implicit val ports: JobSchedulingPorts = new JobSchedulingPorts(config)
  implicit val log: ContextLogger = new ContextLogger

  try {
    DatabaseMigrator.run(config.postgres, onFailRepair = true)

    val jobScheduler = new JobScheduler(config.ecs, config.jobScheduler, JobSchedulerPorts())
    val packagesClient: PackagesClient = {
      val protocolLessHostUri = URI.create(config.pennsieveApi.baseUrl).getHost
      val queuedRequestHttpClient: HttpRequest => Future[HttpResponse] =
        QueueHttpResponder(
          protocolLessHostUri,
          config.pennsieveApi.queueSize,
          config.pennsieveApi.rateLimit
        ).responder

      PackagesClient.httpClient(queuedRequestHttpClient, config.pennsieveApi.baseUrl)
    }
    val pennsieveApiClient =
      new PennsieveApiClientImpl(config.pennsieveApi, ports.jwt, packagesClient, HttpClient())

    val jobMonitor = JobMonitor(
      config.jobMonitor,
      JobMonitorPorts(
        ports.sqsClient,
        pennsieveApiClient,
        ports.db,
        ports.manifestClient,
        jobScheduler,
        config.jobMonitor.queueName
      ),
      config.s3.etlBucket
    )

    val watchDog =
      new WatchDog(
        clusterArn = config.ecs.cluster,
        etlBucket = config.s3.etlBucket,
        uploadQueueName = config.uploadsConsumer.queueName,
        config = config.watchDog,
        ports = WatchDogPorts(
          db = ports.db,
          stopTask = ports.ecsClient.stopTask,
          describeTasks = ports.ecsClient.describeTasks,
          pennsieveApiClient,
          ports.manifestClient,
          jobScheduler
        )
      )

    jobScheduler
      .run(JobSchedulingPorts.finalSink("JobScheduler"))
      .onComplete {
        case Success(_) =>
          jobScheduler.closeQueue
          log.noContext.info("Scheduler completed successfully")
        case Failure(exception) =>
          jobScheduler.closeQueue
          log.noContext.error("Scheduler completed without processing all events", exception)
      }

    // if there are available jobs in the database, start picking them up
    jobScheduler.addJob()

    jobMonitor
      .run(JobSchedulingPorts.finalSink("JobMonitor"))
      .onComplete {
        case Success(_) =>
          log.noContext.info("JobMonitor Stream completed successfully")
        case Failure(exception) =>
          log.noContext.error("JobMonitor completed without processing all events", exception)
      }

    watchDog
      .run(JobSchedulingPorts.finalSink("WatchDog"))
      .onComplete {
        case Success(_) => log.noContext.info("WatchDog completed successfully")
        case Failure(exception) =>
          log.noContext.error("WatchDog completed without processing all events", exception)
      }

    val jobRoutes =
      JobsHandler.routes(
        JobsHandlerPorts(JobsHandlerConfig(config.uploadsConsumer), pennsieveApiClient),
        jobScheduler
      )

    val organizationRoutes = OrganizationsHandler.routes

    val routes: Route =
      Route.seal(jobRoutes ~ organizationRoutes ~ HealthcheckHandler.routes())

    Http()
      .newServerAt(config.host, config.port)
      .bindFlow(routes)
      .flatMap { binding =>
        val localAddress = binding.localAddress
        log.noContext.info(
          s"Server online at http://${localAddress.getHostName}:${localAddress.getPort}"
        )

        binding.whenTerminated
          .map { terminated =>
            system.terminate()
            log.noContext.info(s"Server has terminated with $terminated")
          }
      }
  } catch {
    case e: Throwable =>
      log.noContext.error(e.getMessage)
      system.terminate()
      throw e
  }
}
