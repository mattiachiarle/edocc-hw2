ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.12"

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

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.14.2"
dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala" % "2.14.2"

lazy val commonDependencies = Seq(
  "org.scala-lang.modules" %% "scala-parallel-collections" % scalaParCollVersion,
  "org.scalatest" %% "scalatest" % scalaTestVersion % Test,
  "org.scalatestplus" %% "mockito-4-2" % "3.2.12.0-RC2" % Test,
  "com.typesafe" % "config" % typeSafeConfigVersion,
  "ch.qos.logback" % "logback-classic" % logbackVersion,
  "net.bytebuddy" % "byte-buddy" % netBuddyVersion,
//  "org.apache.hadoop" % "hadoop-common" % "3.3.3",
  "io.circe" %% "circe-core" % "0.14.1",
  "io.circe" %% "circe-generic" % "0.14.1",
  "io.circe" %% "circe-parser" % "0.14.1",
  "org.typelevel" %% "cats-core" % "2.9.0",
  "org.typelevel" %% "jawn-parser" % "1.4.0",
  "ch.qos.logback" % "logback-classic" % "1.4.7",
  "org.yaml" % "snakeyaml" % "2.0",
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-graphx" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.14.2"
)

scalacOptions ++= Seq(
  "-deprecation", // emit warning and location for usages of deprecated APIs
  "--explain-types", // explain type errors in more detail
  "-feature", // emit warning and location for usages of features that should be imported explicitly
  "-Ytasty-reader"
)

compileOrder := CompileOrder.JavaThenScala
//test / fork := true
run / fork := true
//run / javaOptions ++= Seq(
//  "-Xms8G",  "-Xmx100G",
//  "-XX:+UseG1GC")

lazy val root = (project in file("."))
  .settings(
    name := "homework2",
    libraryDependencies ++= commonDependencies
  )
