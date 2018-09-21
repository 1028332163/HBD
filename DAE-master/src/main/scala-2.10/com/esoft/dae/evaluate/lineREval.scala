package com.esoft.dae.evaluate

import com.esoft.dae.dao.BaseDao
import com.esoft.dae.util.{csvUtil, handleUtil}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{SaveMode, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author
  */
class lineREval(taskId: String) extends BaseDao(taskId) {
  def execRegressionEvaluator(sc: SparkContext, basePath: String, inputPath: String,
                              labelCol: String, predictionCol: String,
                              outputPath: String, taskId: String): Unit = {
    /////////构建yarn-spark,从parquet中读取文件
    val sqlContext = new SQLContext(sc)
    val dataFrame = sqlContext.read.parquet(basePath + inputPath)
    //    val dataFrame = csvUtil.readCsv(sc,sqlContext,basePath+inputPath)
    viewDf(dataFrame, "dataFrame")
    //////////////////参数处理

    val dfToUse = dataFrame

    ////////////////////将算法作用于数据集
    val regressionEvaluator = new RegressionEvaluator()
      .setLabelCol(labelCol)
      .setPredictionCol(predictionCol)
    ////////////////////返回页面需要的df
    regressionEvaluator.setMetricName("rmse")
    val fitRmse = regressionEvaluator.evaluate(dfToUse)
    regressionEvaluator.setMetricName("mse")
    val fitMse = regressionEvaluator.evaluate(dfToUse)
    regressionEvaluator.setMetricName("r2")
    val fitR2 = regressionEvaluator.evaluate(dfToUse)
    regressionEvaluator.setMetricName("mae")
    val fitMae = regressionEvaluator.evaluate(dfToUse)
    println(fitRmse, fitMse, fitR2, fitMae)
    val summaryDf = sqlContext.createDataFrame(
      Seq(("rmse", fitRmse), ("mse", fitMse), ("r2", fitR2), ("mae", fitMae))
    ).toDF("summaryType", "value")
    summaryDf.show()
    summaryDf.printSchema()
    //残差值
    val residualRdd = dfToUse.select(labelCol, predictionCol).map(row => Row(row(0).toString.toDouble - row(1).toString.toDouble))
    val residualDf = handleUtil.indexDataFrame(sqlContext.createDataFrame(residualRdd, StructType(Array(StructField("residual", DoubleType, nullable = true)))))
    residualDf.show()
    residualDf.printSchema()


    ////////////////////保存结果及保存json
    val summaryDfPath = handleUtil.getDfShowPath(outputPath) + "1.parquet"
    summaryDf.write.format("parquet")
      .mode(SaveMode.Overwrite)
      .save(basePath + summaryDfPath)
    updateTaskInsData(taskId.toInt, summaryDfPath, summaryDf.dtypes, TWO_TABLE_LEFT, "no json")

    val residualDfPath = handleUtil.getDfShowPath(outputPath) + "2.parquet"
    residualDf.write.format("parquet")
      .mode(SaveMode.Overwrite)
      .save(basePath + residualDfPath)
    updateTaskInsData(taskId.toInt, residualDfPath, residualDf.dtypes, TWO_TABLE_RIGHT, "no json")

    super.flagSparked(taskId.toInt, outputPath, "no head", "no json")
  }

}

object lineREval {

  def main(args: Array[String]): Unit = {
    val basePath = ""
    val inputPath = "data/lineREval.csv"

    val labelCol = "label"
    val predictionCol = "prediction"

    val outputPath = "/out/20161206/p1549_1481002352573.parquet"
    val taskId = "182"


    //    val args = Array(basePath, inputPath,
    //      labelCol, predictionCol,
    //      outputPath, taskId: String)
    exec(handleUtil.getContext("lineREval"), args)

  }

  def exec(sc: SparkContext, args: Array[String]): Unit = {
    implicit lazy val formats = org.json4s.jackson.Serialization.formats(org.json4s.NoTypeHints)
    case class ArgVO(inputPath: String,
                              labelCol: String, predictionCol: String,
                              outputPath: String)

    val executer = new lineREval(args.last)
    try {
      args.foreach { x => executer.logger.info("inArg-" + x) }
      val basePath = args(0)
      val ArgVO(inputPath: String,
                              labelCol: String, predictionCol: String,
                              outputPath: String)
      = org.json4s.jackson.Serialization.read[ArgVO](args(1))
      val taskId = args(2)
      //executer.checkArgsNum(6, args.length)
      executer.execRegressionEvaluator(sc: SparkContext, basePath: String, inputPath: String,
                              labelCol: String, predictionCol: String,
                              outputPath: String, taskId: String)


    } catch {
      case ex: Throwable => executer.handleException(args(args.length - 1).toInt, ex, "lineREval")
    }
  }

}
