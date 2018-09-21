package com.esoft.dae.statistics

import com.esoft.dae.dao.BaseDao
import org.apache.spark.sql.{SaveMode, functions, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import com.esoft.dae.util.{handleUtil, csvUtil}
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

/**
  * @author
  */
class histogram(taskId: String) extends BaseDao(taskId) {

  def exechistogram(sc: SparkContext, basePath: String, inputPath: String,
                    featuresCol: String,
                    outputPath: String, taskId: String): Unit = {
    ///////////构建yarn-spark,从parquet中读取文件
    val sqlContext = new SQLContext(sc)
    val dataFrame = sqlContext.read.parquet(basePath + inputPath)
    //    val dataFrame = csvUtil.readCsv(sc, sqlContext, basePath + inputPath)
    viewDf(dataFrame, "dataFrame")
    //////////////////参数处理
    val cols = featuresCol.split(",")
    ////////////////////将算法作用于数据集
    val minMax = cols.map {
      col =>
        val colDf = dataFrame.select(col).na.drop() //只取有效值
      val min = colDf.select(functions.min(col)).first.get(0).toString
        val max = colDf.select(functions.max(col)).first.get(0).toString
        SummaryVO(col, min, max)
    }

    implicit lazy val formats = Serialization.formats(NoTypeHints)
    val json = Serialization.write(minMax)
    logger.info("finalJson:" + json)

    //写入数据
    val finalDf = dataFrame.select("rownum",cols:_*)
    finalDf.write.format("parquet").mode(SaveMode.Overwrite).save(basePath + outputPath)
    super.flagSparked(taskId.toInt, outputPath, handleUtil.getHeadContent(finalDf.dtypes), json)

  }

  case class SummaryVO(colName: String, min: String, max: String)

}

object histogram {

  def main(args: Array[String]): Unit = {
    val basePath = ""
    val inputPath = "data/lineRegression.csv"

    val featuresCol = "col2,col1"

    val outputPath = ""
    val taskId = "182"

    //
    //    val args = Array(basePath, inputPath,
    //      featuresCol,
    //      outputPath, taskId: String)
    //    val sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local"))
    //    exec(sc, args)
    exec(handleUtil.getContext("histogram"), args)

  }

  def exec(sc: SparkContext, args: Array[String]): Unit = {
    implicit lazy val formats = org.json4s.jackson.Serialization.formats(org.json4s.NoTypeHints)
    case class ArgVO(inputPath: String,
                    featuresCol: String,
                    outputPath: String)

    val executer = new histogram(args.last)
    try {
      args.foreach { x => executer.logger.info("inArg-" + x) }
      val basePath = args(0)
      val ArgVO(inputPath: String,
                    featuresCol: String,
                    outputPath: String)
      = org.json4s.jackson.Serialization.read[ArgVO](args(1))
      val taskId = args(2)
      //executer.checkArgsNum(5, args.length)
      executer.exechistogram(sc: SparkContext, basePath: String, inputPath: String,
                    featuresCol: String,
                    outputPath: String, taskId: String)


    } catch {
      case ex: Throwable => executer.handleException(args(args.length - 1).toInt, ex, "histogram")
    }
  }

}
