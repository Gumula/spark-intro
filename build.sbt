name := "spark-intro"

version := "0.1"

scalaVersion := "2.11.11"

val sparkVersion = "2.2.0"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % sparkVersion,// % "provided",
  "org.apache.spark" % "spark-sql_2.11" % sparkVersion,// % "provided",
  "com.typesafe" % "config" % "1.3.1",

  "org.scalatest" % "scalatest_2.11" % "3.0.4" % "test"
)