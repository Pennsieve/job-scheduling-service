cancelable in Global := true

// Shared settings
ThisBuild / organization := "com.pennsieve"
ThisBuild / scalaVersion := "2.12.11"
ThisBuild / scalacOptions ++= Seq(
  "-language:implicitConversions",
  "-language:postfixOps",
  "-language:reflectiveCalls",
  "-Ypartial-unification",
  "-Xmax-classfile-name", "100",
  "-feature",
  "-deprecation",
)
ThisBuild / resolvers ++= Seq(
  "Pennsieve Releases" at "https://nexus.pennsieve.cc/repository/maven-releases",
  "Pennsieve Snapshots" at "https://nexus.pennsieve.cc/repository/maven-snapshots",
)

ThisBuild / credentials += Credentials("Sonatype Nexus Repository Manager",
  "nexus.pennsieve.cc",
  sys.env("PENNSIEVE_NEXUS_USER"),
  sys.env("PENNSIEVE_NEXUS_PW")
)

// Temporarily disable Coursier because parallel builds fail on Jenkins.
// See https://app.clickup.com/t/a8ned9
ThisBuild / useCoursier := false

ThisBuild / javaOptions += "-Duser.timezone=UTC"

ThisBuild / Test / fork := true
ThisBuild / Test / testForkedParallel := true

ThisBuild / Test / javaOptions ++=
  sys.props.get("testDebug")
    .map(_ => Seq("-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005"))
    .getOrElse(Seq.empty[String])

ThisBuild / version := sys.props.get("version").getOrElse("SNAPSHOT")

lazy val headerLicenseValue = Some(HeaderLicense.Custom(
  s"Copyright (c) [2018] - [${java.time.Year.now.getValue}] Pennsieve, Inc. All Rights Reserved."
))
lazy val headerMappingsValue = HeaderFileType.scala -> HeaderCommentStyle.cppStyleLineComment

// Dependency versions
lazy val AkkaHttpVersion = "10.1.11"
lazy val AkkaVersion = "2.6.5"
lazy val AuthMiddlewareVersion = "5.1.3"
lazy val AwsVersion = "1.11.414"
lazy val CatsVersion = "1.5.0"
lazy val CirceVersion = "0.11.1"
lazy val CoreVersion = "190-9da55c4"
lazy val DockerItVersion = "0.9.7"
lazy val EnumeratumVersion = "1.5.14"
lazy val LogbackVersion = "1.2.3"
lazy val PureConfigVersion = "0.9.1"
lazy val ScalaLoggingVersion = "3.9.2"
lazy val SlickVersion = "3.3.2"
lazy val SlickPgVersion = "0.17.3"
lazy val ServiceUtilitiesVersion = "8-9751ee3"
lazy val UtilitiesVersion = "4-55953e4"

// Shared dependencies
ThisBuild / libraryDependencies ++= Seq(
  "com.typesafe.scala-logging" %% "scala-logging"     % ScalaLoggingVersion,

  "com.pennsieve"              %% "core-models"       % CoreVersion,

  "com.typesafe.akka"          %% "akka-http"         % AkkaHttpVersion,
  "com.typesafe.akka"          %% "akka-stream-typed" % AkkaVersion,

  "com.beachape"               %% "enumeratum"        % EnumeratumVersion,
  "com.beachape"               %% "enumeratum-circe"  % EnumeratumVersion,

  "io.circe"                   %% "circe-core"        % CirceVersion,
  "io.circe"                   %% "circe-generic"     % CirceVersion,
  "io.circe"                   %% "circe-java8"       % CirceVersion,
  "io.circe"                   %% "circe-jawn"        % CirceVersion,

  "org.typelevel"              %% "cats-core"         % CatsVersion,
)

// project definitions
lazy val client = project
  .enablePlugins(AutomateHeaderPlugin)
  .dependsOn(commons)
  .settings(
    name := "job-scheduling-service-client",
    headerLicense := headerLicenseValue,
    headerMappings := headerMappings.value + headerMappingsValue,
    publishTo := {
      val nexus = "https://nexus.pennsieve.cc/repository"
      if (isSnapshot.value) {
        Some("Nexus Realm" at s"$nexus/maven-snapshots")
      } else {
        Some("Nexus Realm" at s"$nexus/maven-releases")
      }
    },
    publishMavenStyle := true,
    Compile / guardrailTasks := List(
      Client(file("./swagger/job-scheduling-service.yml"), pkg="com.pennsieve.jobscheduling.clients.generated")
    )
  )

lazy val commons = project
  .enablePlugins(AutomateHeaderPlugin)
  .settings(
    name := "job-scheduling-service-commons",
    headerLicense := headerLicenseValue,
    headerMappings := headerMappings.value + headerMappingsValue,
    publishTo := {
      val nexus = "https://nexus.pennsieve.cc/repository"

      if (isSnapshot.value) {
        Some("Nexus Realm" at s"$nexus/maven-snapshots")
      } else {
        Some("Nexus Realm" at s"$nexus/maven-releases")
      }
    },
    publishMavenStyle := true
  )

lazy val server = project
  .enablePlugins(AutomateHeaderPlugin)
  .enablePlugins(DockerPlugin)
  .dependsOn(client % "test->compile", commons)
  .settings(
    name := "job-scheduling-service",
    headerLicense := headerLicenseValue,
    headerMappings := headerMappings.value + headerMappingsValue,
    Compile / guardrailTasks := List(
      Server(file("./swagger/job-scheduling-service.yml"), pkg="com.pennsieve.jobscheduling.server.generated")
    ),
    assembly / test := {},
    libraryDependencies ++= Seq(
      "com.lightbend.akka" %% "akka-stream-alpakka-sqs" % "1.0-M1",

      "com.amazonaws" % "aws-java-sdk-batch" % AwsVersion,
      "com.amazonaws" % "aws-java-sdk-core" % AwsVersion exclude ("commons-logging", "commons-logging"),
      "com.amazonaws" % "aws-java-sdk-ecs" % AwsVersion,
      "com.amazonaws" % "aws-java-sdk-s3" % AwsVersion,
      "com.amazonaws" % "aws-java-sdk-sqs" % AwsVersion,

      "com.pennsieve" %% "service-utilities" % ServiceUtilitiesVersion,
      "com.pennsieve" %% "utilities" % UtilitiesVersion,

      "com.pennsieve" %% "auth-middleware" % AuthMiddlewareVersion,
      "com.pennsieve" %% "core-clients" % CoreVersion,

      "org.apache.commons" % "commons-io" % "1.3.2",

      "com.github.seratch" %% "awscala" % "0.6.0" exclude ("commons-logging", "commons-logging"),

      "ch.qos.logback" % "logback-classic" % LogbackVersion,
      "ch.qos.logback" % "logback-core" % LogbackVersion,
      "net.logstash.logback" % "logstash-logback-encoder" % "5.2",

      "ch.megard" %% "akka-http-cors" % "0.3.0",

      "com.github.pureconfig" %% "pureconfig" % PureConfigVersion,

      "com.typesafe.slick" %% "slick" % SlickVersion,
      "com.typesafe.slick" %% "slick-hikaricp" % SlickVersion,

      "com.github.tminglei" %% "slick-pg" % SlickPgVersion,
      "com.github.tminglei" %% "slick-pg_circe-json" % SlickPgVersion,

      "com.beachape" %% "enumeratum" % EnumeratumVersion,
      "com.beachape" %% "enumeratum-circe" % EnumeratumVersion,

      "org.postgresql" % "postgresql" % "42.2.4",

      "io.scalaland" %% "chimney" % "0.2.1",

      "com.pennsieve" %% "utilities" % UtilitiesVersion % Test classifier "tests",
      "com.whisk" %% "docker-testkit-scalatest" % DockerItVersion % Test,
      "com.whisk" %% "docker-testkit-impl-spotify" % DockerItVersion % Test,
      "org.scalatest" %% "scalatest"% "3.0.5" % Test,
      "com.typesafe.akka" %% "akka-http-testkit" % AkkaHttpVersion % Test,
      "com.typesafe.akka" %% "akka-actor-testkit-typed" % AkkaVersion % Test,
      "com.typesafe.akka" %% "akka-stream-testkit" % AkkaVersion % Test,
    ),

    dependencyOverrides ++= Seq(
      "io.circe" %% "circe-core" % CirceVersion,
      "io.circe" %% "circe-generic" % CirceVersion,
      "io.circe" %% "circe-java8" % CirceVersion,
      "io.circe" %% "circe-jawn" % CirceVersion,
    ),

    coverageExcludedPackages := "com.pennsieve.jobscheduling.server\\..*;"
      + "com.pennsieve.jobscheduling.Server;"
      + "com.pennsieve.jobscheduling.ServiceConfig;"
      + "com.pennsieve.jobscheduling.DatabaseMigrator;"
      + "com.pennsieve.jobscheduling.ETLLogger.*;"
      + "com.pennsieve.jobscheduling.model.EventualResult.*;"
      + "com.pennsieve.jobscheduling.model.OffsetDateTimeEncoder;"
      + "com.pennsieve.jobscheduling.handlers.HealthcheckHandler;"
      + "com.pennsieve.jobscheduling.monitor.JobMonitorPorts;"
      + "com.pennsieve.jobscheduling.monitor.CloudwatchMessage;"
      + "com.pennsieve.jobscheduling.pusher.JobPusherPorts;"
      + "com.pennsieve.jobscheduling.scheduler.JobSchedulerPorts;"
      + "com.pennsieve.jobscheduling.watchdog.WatchDogPorts;"
      + "com.pennsieve.jobscheduling.model.InvalidCursorException;"
      + "com.pennsieve.jobscheduling.watchdog.FailedToStopTaskException;"
      + "com.pennsieve.jobscheduling.watchdog.StoppedTaskWithoutJobException;"
      + "com.pennsieve.jobscheduling.watchdog.NoJobIdException;"
      + "com.pennsieve.jobscheduling.watchdog.WatchDogException;"
      + "com.pennsieve.jobscheduling.db.DatabaseClientFlows;"
      + "com.pennsieve.jobscheduling.clients\\..*;"
      + "com.pennsieve.jobscheduling.errors\\..*;"
      + "com.pennsieve.jobscheduling.db.PostgresProfile",
    coverageMinimum := 85,
    coverageFailOnMinimum := true,

    scalafmtOnCompile := true,

    docker / dockerfile := {
      val artifact: File = assembly.value
      val artifactTargetPath = s"/app/${artifact.name}"
      new Dockerfile {
        from("pennsieve/java-cloudwrap:10-jre-slim-0.5.9")
        copy(artifact, artifactTargetPath, chown="pennsieve:pennsieve")
        copy(baseDirectory.value / "bin" / "run.sh", "/app/run.sh", chown="pennsieve:pennsieve")
        run("wget", "-qO", "/app/newrelic.jar", "http://download.newrelic.com/newrelic/java-agent/newrelic-agent/current/newrelic.jar")
        run("mkdir", "-p", "/home/pennsieve/.postgresql")
        run("wget", "-qO", "/home/pennsieve/.postgresql/root.crt", "https://s3.amazonaws.com/rds-downloads/rds-ca-2019-root.pem")
        env("RUST_BACKTRACE", "1")
        cmd("--service", "job-scheduling-service", "exec", "app/run.sh", artifactTargetPath)
      }
    },
    docker / imageNames := Seq(
      ImageName("pennsieve/job-scheduling-service:latest")
    )
  )

lazy val root = (project in file("."))
  .aggregate(server, client)
