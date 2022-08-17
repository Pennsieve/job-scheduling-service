resolvers ++= Seq(
  "pennsieve-maven-proxy" at "https://nexus.pennsieve.cc/repository/maven-public",
  Resolver.url("pennsieve-ivy-proxy", url("https://nexus.pennsieve.cc/repository/ivy-public/"))( Patterns("[organization]/[module]/(scala_[scalaVersion]/)(sbt_[sbtVersion]/)[revision]/[type]s/[artifact](-[classifier]).[ext]") ),
)

credentials += Credentials(
  "Sonatype Nexus Repository Manager",
  "nexus.pennsieve.cc",
  sys.env("PENNSIEVE_NEXUS_USER"),
  sys.env("PENNSIEVE_NEXUS_PW")
)

// Temporarily disable Coursier because parallel builds fail on Jenkins.
// See https://app.clickup.com/t/a8ned9
ThisBuild / useCoursier := false

addSbtPlugin("se.marcuslonnberg" % "sbt-docker" % "1.5.0")

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.7")

addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.4.2")

addSbtPlugin("dev.guardrail" % "sbt-guardrail" % "0.70.0.2")

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.5.1")

addSbtPlugin("de.heikoseeberger" % "sbt-header" % "5.1.0")
