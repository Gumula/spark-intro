
ThisBuild / scalaVersion     := "2.12.12"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "pl.allegro.workshop.sparkintro"
ThisBuild / organizationName := "allegro"

val sparkVersion = "3.1.0"

lazy val root = (project in file("."))
  .settings(
    name := "spark-intro",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "com.typesafe" % "config" % "1.4.1",

      "org.scalatest" %% "scalatest" % "3.2.3" % "test"
    )
  )