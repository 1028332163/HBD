package com.esoft.dae.jobserver.dataSource

import org.slf4j.LoggerFactory

import scala.util.Try
import spark.jobserver.api.{SingleProblem, ValidationProblem, JobEnvironment, SparkJob}
import org.scalactic._
import com.typesafe.config.Config
import org.apache.spark.SparkContext


/**
  * @author liuzw
  */

object DataSourceParquet extends SparkJob {
  type JobData = Seq[String]
  type JobOutput = Unit
  val logger = LoggerFactory.getLogger(getClass)

  def runJob(sc: SparkContext, runtime: JobEnvironment, args: JobData): JobOutput = {
    logger.info("++++++++++++++++++++++++++++++++++")
    logger.info("+++uniFlag:0815")
    logger.info("++++++++++++++++++++++++++++++++++")
    com.esoft.dae.dataSource.DataSourceParquet.exec(sc, args.toArray)
  }

  def validate(sc: SparkContext, runtime: JobEnvironment, config: Config):
  JobData Or Every[ValidationProblem] = {
    println(config.getString("input.string"))
    val a =
      Try(config.getString("input.string").split("-=-").toSeq)
    val b = a.map(words => Good(words))
    b.getOrElse(Bad(One(SingleProblem("No input.string param"))))
  }

}
