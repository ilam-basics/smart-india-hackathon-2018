name := "smart-india-hackathon"

organization := "gov.dot.sih"

version := "0.0.1"

scalaVersion := "2.11.8"

lazy val hadoopVersion = "2.7.4"
lazy val sparkVersion = "2.1.0"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "2.1.0",
  "org.apache.spark" % "spark-sql_2.11" % "2.1.0",
  "org.mongodb.spark" % "mongo-spark-connector_2.11" % "2.2.0",
  "com.google.code.gson" % "gson" % "2.8.2"
)
