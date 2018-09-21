package com.esoft.dae.jobserver

import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.scalactic._
import spark.jobserver.api.{SparkJob, SingleProblem, ValidationProblem, JobEnvironment}

import scala.util.Try

/**
  * Created by asus on 2017/3/7.
  */
object ReadCsv extends SparkJob {
  type JobData = Seq[String]
  type JobOutput = String

  def runJob(sc: SparkContext, runtime: JobEnvironment, arg: JobData): JobOutput = {
    com.esoft.dae.ReadCsv.exec(sc, arg.head, arg(1), arg(2))
  }

  def validate(sc: SparkContext, runtime: JobEnvironment, config: Config):
  JobData Or Every[ValidationProblem] = {
    val a =
      Try(config.getString("input.string").split("-=-").toSeq)
    val b = a.map(words => Good(words))
    b.getOrElse(Bad(One(SingleProblem("No input.string param"))))
  }

}
