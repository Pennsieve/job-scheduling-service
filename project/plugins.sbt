// Temporarily disable Coursier because parallel builds fail on Jenkins.
// See https://app.clickup.com/t/a8ned9
ThisBuild / useCoursier := false

addSbtPlugin("se.marcuslonnberg" % "sbt-docker" % "1.11.0")

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.7")

addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.4.2")

addSbtPlugin("dev.guardrail" % "sbt-guardrail" % "0.70.0.2")

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "2.3.1")

addSbtPlugin("de.heikoseeberger" % "sbt-header" % "5.1.0")

addSbtPlugin("no.arktekk.sbt" % "aether-deploy" % "0.31.0")
