import CrossCompilationUtil.{
  getScalacOptions,
  getVersion,
  handle212OnlyDependency,
  scalaVersionMatch
}
Global / cancelable := true

lazy val scala212 = "2.12.11"
lazy val scala213 = "2.13.8"
lazy val supportedScalaVersions = List(scala212, scala213)

// Shared settings
ThisBuild / organization := "com.pennsieve"
ThisBuild / scalaVersion := scala213
ThisBuild / resolvers ++= Seq(
  "Pennsieve Releases" at "https://nexus.pennsieve.cc/repository/maven-releases",
  "Pennsieve Snapshots" at "https://nexus.pennsieve.cc/repository/maven-snapshots"
)

ThisBuild / credentials += Credentials(
  "Sonatype Nexus Repository Manager",
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
  sys.props
    .get("testDebug")
    .map(_ => Seq("-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005"))
    .getOrElse(Seq.empty[String])

ThisBuild / version := sys.props.get("version").getOrElse("SNAPSHOT")

lazy val headerLicenseValue = Some(
  HeaderLicense.Custom(
    s"Copyright (c) [2018] - [${java.time.Year.now.getValue}] Pennsieve, Inc. All Rights Reserved."
  )
)
lazy val headerMappingsValue = HeaderFileType.scala -> HeaderCommentStyle.cppStyleLineComment

// Dependency versions
lazy val akkaHttp212Version = "10.1.11"
lazy val akkaHttp213Version = "10.2.7"
lazy val akka212Version = "2.6.5"
lazy val akka213Version = "2.6.8"
lazy val authMiddlewareVersion = "5.1.3"
lazy val awsVersion = "2.11.14"
lazy val cats212Version = "1.5.0"
lazy val cats213Version = "2.6.1"
lazy val circe212Version = "0.11.1"
lazy val circe213Version = "0.14.1"
lazy val coreVersion = "191-fe6a5c7"
lazy val dockerItVersion = "0.9.9"
lazy val enumeratum212Version = "1.5.14"
lazy val enumeratum213Version = "1.7.0"
lazy val logbackVersion = "1.2.3"
lazy val pureConfigVersion = "0.17.1"
lazy val scalaLoggingVersion = "3.9.2"
lazy val slickVersion = "3.3.3"
lazy val slickPgVersion = "0.20.3"
lazy val serviceUtilitiesVersion = "8-9751ee3"
lazy val utilitiesVersion = "4-55953e4"

lazy val circeVersion = SettingKey[String]("circeVersion")
lazy val enumeratumVersion = SettingKey[String]("enumeratumVersion")
lazy val catsVersion = SettingKey[String]("catsVersion")
lazy val akkaHttpVersion = SettingKey[String]("akkaHttpVersion")
lazy val akkaVersion = SettingKey[String]("akkaVersion")

lazy val sharedEnumeratumDependencies =
  Seq("com.beachape" %% "enumeratum", "com.beachape" %% "enumeratum-circe")

lazy val sharedCirceDependencies =
  Seq("io.circe" %% "circe-core", "io.circe" %% "circe-generic", "io.circe" %% "circe-jawn")

lazy val sharedCatsDependencies = Seq("org.typelevel" %% "cats-core")

lazy val sharedAkkaDependencies = Seq("com.typesafe.akka" %% "akka-stream-typed")

lazy val sharedAkkaHttpDependencies = Seq("com.typesafe.akka" %% "akka-http")

// Shared dependencies
ThisBuild / libraryDependencies ++= Seq(
  "com.typesafe.scala-logging" %% "scala-logging" % scalaLoggingVersion,
  "com.pennsieve" %% "core-models" % coreVersion
)

// project definitions
lazy val client = project
  .enablePlugins(AutomateHeaderPlugin)
  .dependsOn(commons)
  .settings(
    name := "job-scheduling-service-client",
    scalacOptions ++= getScalacOptions(scalaVersion.value),
    crossScalaVersions := supportedScalaVersions,
    circeVersion := getVersion(scalaVersion.value, circe212Version, circe213Version),
    enumeratumVersion := getVersion(scalaVersion.value, enumeratum212Version, enumeratum213Version),
    catsVersion := getVersion(scalaVersion.value, cats212Version, cats213Version),
    akkaVersion := getVersion(scalaVersion.value, akka212Version, akka213Version),
    akkaHttpVersion := getVersion(scalaVersion.value, akkaHttp212Version, akkaHttp213Version),
    libraryDependencies ++= sharedAkkaDependencies.map(_ % akkaVersion.value),
    libraryDependencies ++= sharedAkkaHttpDependencies.map(_ % akkaHttpVersion.value),
    libraryDependencies ++= sharedEnumeratumDependencies.map(_ % enumeratumVersion.value),
    libraryDependencies ++= sharedCirceDependencies.map(_ % circeVersion.value),
    libraryDependencies ++= handle212OnlyDependency(
      scalaVersion.value,
      "io.circe" %% "circe-java8" % circeVersion.value
    ),
    libraryDependencies ++= sharedCatsDependencies.map(_ % catsVersion.value),
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
    Compile / guardrailTasks := scalaVersionMatch(
      scalaVersion.value,
      List(
        ScalaClient(
          file("./swagger/job-scheduling-service.yml"),
          pkg = "com.pennsieve.jobscheduling.clients.generated",
          modules = List("akka-http", "circe-0.11")
        )
      ),
      List(
        ScalaClient(
          file("./swagger/job-scheduling-service.yml"),
          pkg = "com.pennsieve.jobscheduling.clients.generated"
        )
      )
    )
  )

lazy val commons = project
  .enablePlugins(AutomateHeaderPlugin)
  .settings(
    name := "job-scheduling-service-commons",
    scalacOptions ++= getScalacOptions(scalaVersion.value),
    crossScalaVersions := supportedScalaVersions,
    circeVersion := getVersion(scalaVersion.value, circe212Version, circe213Version),
    enumeratumVersion := getVersion(scalaVersion.value, enumeratum212Version, enumeratum213Version),
    catsVersion := getVersion(scalaVersion.value, cats212Version, cats213Version),
    akkaVersion := getVersion(scalaVersion.value, akka212Version, akka213Version),
    akkaHttpVersion := getVersion(scalaVersion.value, akkaHttp212Version, akkaHttp213Version),
    libraryDependencies ++= sharedAkkaDependencies.map(_ % akkaVersion.value),
    libraryDependencies ++= sharedAkkaHttpDependencies.map(_ % akkaHttpVersion.value),
    libraryDependencies ++= sharedEnumeratumDependencies.map(_ % enumeratumVersion.value),
    libraryDependencies ++= sharedCirceDependencies.map(_ % circeVersion.value),
    libraryDependencies ++= handle212OnlyDependency(
      scalaVersion.value,
      "io.circe" %% "circe-java8" % circeVersion.value
    ),
    libraryDependencies ++= sharedCatsDependencies.map(_ % catsVersion.value),
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
  .dependsOn(client % "test", commons)
  .settings(
    name := "job-scheduling-service",
    scalacOptions ++= getScalacOptions(scalaVersion.value),
    headerLicense := headerLicenseValue,
    headerMappings := headerMappings.value + headerMappingsValue,
    Compile / guardrailTasks := List(
      ScalaServer(
        file("./swagger/job-scheduling-service.yml"),
        pkg = "com.pennsieve.jobscheduling.server.generated"
      )
    ),
    assembly / test := {},
    circeVersion := getVersion(scalaVersion.value, circe212Version, circe213Version),
    enumeratumVersion := getVersion(scalaVersion.value, enumeratum212Version, enumeratum213Version),
    catsVersion := getVersion(scalaVersion.value, cats212Version, cats213Version),
    akkaVersion := getVersion(scalaVersion.value, akka212Version, akka213Version),
    akkaHttpVersion := getVersion(scalaVersion.value, akkaHttp212Version, akkaHttp213Version),
    libraryDependencies ++= sharedAkkaDependencies.map(_ % akkaVersion.value),
    libraryDependencies ++= sharedAkkaHttpDependencies.map(_ % akkaHttpVersion.value),
    libraryDependencies ++= sharedEnumeratumDependencies.map(_ % enumeratumVersion.value),
    libraryDependencies ++= sharedCirceDependencies.map(_ % circeVersion.value),
    libraryDependencies ++= sharedCatsDependencies.map(_ % catsVersion.value),
    libraryDependencies ++= Seq(
      "com.lightbend.akka" %% "akka-stream-alpakka-sqs" % "2.0.2",
      "software.amazon.awssdk" % "batch" % awsVersion,
      "software.amazon.awssdk" % "ecs" % awsVersion,
      "software.amazon.awssdk" % "s3" % awsVersion,
      "software.amazon.awssdk" % "sqs" % awsVersion,
      "com.pennsieve" %% "service-utilities" % serviceUtilitiesVersion,
      "com.pennsieve" %% "utilities" % utilitiesVersion,
      "com.pennsieve" %% "auth-middleware" % authMiddlewareVersion,
      "com.pennsieve" %% "core-clients" % coreVersion,
      "org.apache.commons" % "commons-io" % "1.3.2",
      // Don't know what this is for. Never used explicitly and tied to v1 of AWS Java SDK
      //"com.github.seratch" %% "awscala" % "0.8.5" exclude ("commons-logging", "commons-logging"),
      "ch.qos.logback" % "logback-classic" % logbackVersion,
      "ch.qos.logback" % "logback-core" % logbackVersion,
      "net.logstash.logback" % "logstash-logback-encoder" % "5.2",
      "ch.megard" %% "akka-http-cors" % "1.1.3",
      "com.github.pureconfig" %% "pureconfig" % pureConfigVersion,
      "com.typesafe.slick" %% "slick" % slickVersion,
      "com.typesafe.slick" %% "slick-hikaricp" % slickVersion,
      "com.github.tminglei" %% "slick-pg" % slickPgVersion,
      "com.github.tminglei" %% "slick-pg_circe-json" % slickPgVersion,
      "org.postgresql" % "postgresql" % "42.2.4",
      "io.scalaland" %% "chimney" % "0.6.1",
      "com.pennsieve" %% "utilities" % utilitiesVersion % Test classifier "tests",
      "com.whisk" %% "docker-testkit-scalatest" % dockerItVersion % Test,
      "com.whisk" %% "docker-testkit-impl-spotify" % dockerItVersion % Test,
      "org.scalatest" %% "scalatest" % "3.2.12" % Test,
      "com.typesafe.akka" %% "akka-http-testkit" % akkaHttpVersion.value % Test,
      "com.typesafe.akka" %% "akka-actor-testkit-typed" % akkaVersion.value % Test,
      "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion.value % Test
    ),
    dependencyOverrides ++= Seq(
      "io.circe" %% "circe-core" % circeVersion.value,
      "io.circe" %% "circe-generic" % circeVersion.value,
      "io.circe" %% "circe-java8" % circeVersion.value,
      "io.circe" %% "circe-jawn" % circeVersion.value
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
    coverageMinimumStmtTotal := 85,
    coverageFailOnMinimum := true,
    scalafmtOnCompile := true,
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "io.netty.versions.properties", _ @_*) => MergeStrategy.discard
      case PathList("codegen-resources", "customization.config", _ @_*) => MergeStrategy.discard
      case PathList("codegen-resources", "examples-1.json", _ @_*) => MergeStrategy.discard
      case PathList("codegen-resources", "paginators-1.json", _ @_*) => MergeStrategy.discard
      case PathList("codegen-resources", "service-2.json", _ @_*) => MergeStrategy.discard
      case PathList("codegen-resources", "waiters-2.json", _ @_*) => MergeStrategy.discard
      case PathList("module-info.class") => MergeStrategy.discard
      case x =>
        val oldStrategy = (assembly / assemblyMergeStrategy).value
        oldStrategy(x)
    },
    docker / dockerfile := {
      val artifact: File = assembly.value
      val artifactTargetPath = s"/app/${artifact.name}"

      // Where Postgres (psql/JDBC) expects to find the trusted CA certificate
      val CA_CERT_LOCATION = "/home/pennsieve/.postgresql/root.crt"

      new Dockerfile {
        from("pennsieve/java-cloudwrap:10-jre-slim-0.5.9")
        copy(artifact, artifactTargetPath, chown = "pennsieve:pennsieve")
        copy(baseDirectory.value / "bin" / "run.sh", "/app/run.sh", chown = "pennsieve:pennsieve")
        run(
          "wget",
          "-qO",
          "/app/newrelic.jar",
          "http://download.newrelic.com/newrelic/java-agent/newrelic-agent/current/newrelic.jar"
        )
        addRaw(
          "https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem",
          CA_CERT_LOCATION,
        )
        user("root")
        run("chmod", "+r", CA_CERT_LOCATION)
        user("pennsieve")
        cmd("--service", "job-scheduling-service", "exec", "app/run.sh", artifactTargetPath)
      }
    },
    docker / imageNames := Seq(ImageName("pennsieve/job-scheduling-service:latest"))
  )

lazy val root = (project in file("."))
  .aggregate(server, client)
  .settings(
    // crossScalaVersions must be set to Nil on the aggregating project
    crossScalaVersions := Nil,
    publish / skip := true
  )
