package com.esoft.dae.statistics

import com.esoft.dae.dao.BaseDao
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.{SparkConf, SparkContext}
import com.esoft.dae.util.{handleUtil, csvUtil}
import org.apache.spark.sql.SQLContext
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization._
import org.json4s.jackson.Serialization
import org.apache.spark.mllib.stat.Statistics

/**
  * @author
  */
class chiSquareTest(taskId: String) extends BaseDao(taskId) {
  def execStatistics(sc: SparkContext, basePath: String, inputPath: String,
                     featuresCol: String, labelCol: String,
                     outputPath: String, taskId: String): Unit = {
    ///////////构建yarn-spark,从parquet中读取文件
    val sqlContext = new SQLContext(sc)
    val dataFrame = sqlContext.read.parquet(basePath + inputPath)
    //    val dataFrame = csvUtil.readCsv(sc, sqlContext, basePath + inputPath)
    viewDf(dataFrame, "dataFrame")
    ////////////////////将算法作用于数据集
    val featuresArr = handleUtil.sortColNames(featuresCol)
    val indexcer = new StringIndexer()
    val initDf =
      indexcer.setInputCol(labelCol).setOutputCol("index_l_" + labelCol).fit(dataFrame).transform(dataFrame)
    val dfToUse = featuresArr.foldLeft(initDf) {
      (initDf, featureCol) =>
        indexcer.setInputCol(featureCol).setOutputCol("index_" + featureCol).fit(initDf).transform(initDf)
    }
    val rddToUse = handleUtil.getLabelPointRdd(dfToUse,
      featuresArr.map(col => "index_" + col).mkString(","), "index_l_" + labelCol)
    val results = Statistics.chiSqTest(rddToUse).map(result => result.toString())

    implicit lazy val formats = Serialization.formats(NoTypeHints)
    val json = write(featuresArr.zip(results).map(result => ShowVO(result._1, result._2)))
    logger.info("finalJson:" + json)
    ////////////////////页面展现用的df
    super.flagSparked(taskId.toInt, outputPath, "no head", json)
  }

  case class ShowVO(colName: String, info: String)

}

object chiSquareTest {

  def main(args: Array[String]): Unit = {
    val basePath = ""
    val inputPath = "data/tree.csv"

    val featuresCol = "col1,col2"
    val labelCol = "label"

    val outputPath = ""
    val taskId = "182"

    //    val args = Array(basePath, inputPath,
    //      featuresCol, labelCol,
    //      outputPath, taskId: String)
    exec(handleUtil.getContext("chiSquareTest"), args)

  }

  def exec(sc: SparkContext, args: Array[String]): Unit = {
    implicit lazy val formats = org.json4s.jackson.Serialization.formats(org.json4s.NoTypeHints)
    case class ArgVO(inputPath: String,
                     featuresCol: String, labelCol: String,
                     outputPath: String)

    val executer = new chiSquareTest(args.last)
    try {
      args.foreach { x => executer.logger.info("inArg-" + x) }
      val basePath = args(0)
      val ArgVO(inputPath: String,
                     featuresCol: String, labelCol: String,
                     outputPath: String)
      = org.json4s.jackson.Serialization.read[ArgVO](args(1))
      val taskId = args(2)
      //executer.checkArgsNum(6, args.length)
      executer.execStatistics(sc: SparkContext, basePath: String, inputPath: String,
                     featuresCol: String, labelCol: String,
                     outputPath: String, taskId: String)


    } catch {
      case ex: Throwable => executer.handleException(args(args.length - 1).toInt, ex, "chiSquareTest")
    }
  }

}
