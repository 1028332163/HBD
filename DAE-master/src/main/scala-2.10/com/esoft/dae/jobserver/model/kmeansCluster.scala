package com.esoft.dae.jobserver.model

import scala.util.Try
import spark.jobserver.api.{SingleProblem, ValidationProblem, JobEnvironment, SparkJob}
import org.scalactic._
import com.typesafe.config.Config
import org.apache.spark.SparkContext


/**
  * @author liuzw
  */

object kmeansCluster extends SparkJob {
  type JobData = Seq[String]
  type JobOutput = Unit

  def runJob(sc: SparkContext, runtime: JobEnvironment, args: JobData): JobOutput = {
    com.esoft.dae.model.kmeansCluster.exec(sc, args.toArray)
  }

  def validate(sc: SparkContext, runtime: JobEnvironment, config: Config):
  JobData Or Every[ValidationProblem] = {
    val a =
      Try(config.getString("input.string").split("-=-").toSeq)
    val b = a.map(words => Good(words))
    b.getOrElse(Bad(One(SingleProblem("No input.string param"))))
  }

}
