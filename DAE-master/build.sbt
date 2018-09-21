name := "DAE_V4"

version := "1.0"

scalaVersion := "2.10.6"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "1.6.1",
  "org.apache.spark" % "spark-sql_2.10" % "1.6.1",
  "org.apache.spark" % "spark-mllib_2.10" % "1.6.1",
  "mysql" % "mysql-connector-java" % "5.1.34",
  "com.databricks" % "spark-csv_2.10" % "1.4.0",
  "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.6.1",
  "spark.jobserver" %% "job-server-api" % "0.7.0-SNAPSHOT_cdh-5.7",
  "spark.jobserver" %% "job-server-extras" % "0.7.0-SNAPSHOT_cdh-5.7" % "provided",
  "org.scalactic" %% "scalactic" % "3.0.1",
  "org.scalatest" %% "scalatest" % "3.0.1"% "test"

)