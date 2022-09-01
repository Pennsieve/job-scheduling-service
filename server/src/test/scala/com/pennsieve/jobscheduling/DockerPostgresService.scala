// Copyright (c) [2018] - [2022] Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.jobscheduling

import com.pennsieve.jobscheduling.TestPostgresConfiguration.advertisedPort
import com.whisk.docker._

import java.sql.DriverManager
import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.Try

trait DockerPostgresService extends DockerKit {

  val postgresConfiguration: PostgresConfig =
    TestPostgresConfiguration.freshPostgresConfiguration

  val postgresContainer: DockerContainer = DockerContainer("postgres:9.5.10")
    .withPorts((advertisedPort, Some(postgresConfiguration.port)))
    .withEnv(
      s"POSTGRES_USER=${postgresConfiguration.user}",
      s"POSTGRES_PASSWORD=${postgresConfiguration.password}"
    )
    .withReadyChecker(
      new PostgresReadyChecker(postgresConfiguration)
        .looped(15, 1.second)
    )

  abstract override def dockerContainers: List[DockerContainer] =
    postgresContainer :: super.dockerContainers

}

class PostgresReadyChecker(configuration: PostgresConfig) extends DockerReadyChecker {

  override def apply(
    container: DockerContainerState
  )(implicit
    docker: DockerCommandExecutor,
    executionContext: ExecutionContext
  ): Future[Boolean] = {
    container
      .getPorts()
      .map { ports =>
        val ready: Try[Boolean] = Try {
          Class.forName(configuration.driver)

          Option(
            DriverManager.getConnection(
              configuration
                .copy(host = docker.host, port = configuration.port)
                .jdbcURL,
              configuration.user,
              configuration.password
            )
          ).map(_.close).isDefined
        }

        ready.getOrElse(false)
      }
  }
}
