ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"

val scalaTestVersion = "3.2.11"
val guavaVersion = "31.1-jre"
val typeSafeConfigVersion = "1.4.2"
val logbackVersion = "1.2.10"
val sfl4sVersion = "2.0.0-alpha5"
val graphVizVersion = "0.18.1"
val netBuddyVersion = "1.14.4"
val catsVersion = "2.9.0"
val apacheCommonsVersion = "2.13.0"
val jGraphTlibVersion = "1.5.2"
val scalaParCollVersion = "1.0.4"
val guavaAdapter2jGraphtVersion = "1.5.2"
val sparkVersion = "3.4.1"

lazy val commonDependencies = Seq(
  "org.scalatest" %% "scalatest" % scalaTestVersion % Test,
  "org.scalatestplus" %% "mockito-4-2" % "3.2.12.0-RC2" % Test,
  "org.scalamock" %% "scalamock" % "5.2.0" % Test,
  "com.typesafe" % "config" % typeSafeConfigVersion,
  "ch.qos.logback" % "logback-classic" % logbackVersion,
  "io.circe" %% "circe-core" % "0.14.1",
  "io.circe" %% "circe-generic" % "0.14.1",
  "io.circe" %% "circe-parser" % "0.14.1",
  "ch.qos.logback" % "logback-classic" % "1.4.7",
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-graphx" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
)

assembly / assemblyShadeRules := Seq(
  ShadeRule.rename("org.typelevel.cats.**" -> "repackaged.org.typelevel.cats.@1").inAll,
  ShadeRule.rename("cats.**" -> "repackaged.cats.@1").inAll,
)

scalacOptions ++= Seq(
  "-deprecation", // emit warning and location for usages of deprecated APIs
  "-feature", // emit warning and location for usages of features that should be imported explicitly
)

compileOrder := CompileOrder.JavaThenScala
test / fork := true
run / fork := true
run / javaOptions ++= Seq(
  "-Xms8G",  "-Xmx100G",
  "-XX:+UseG1GC")

lazy val root = (project in file("."))
  .settings(
    name := "homework2",
    libraryDependencies ++= commonDependencies
  )
