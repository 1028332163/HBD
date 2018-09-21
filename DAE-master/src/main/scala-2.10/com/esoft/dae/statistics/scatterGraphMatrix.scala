package com.esoft.dae.statistics

import com.esoft.dae.dao.BaseDao
import org.apache.spark.{SparkConf, SparkContext}
import com.esoft.dae.util.{handleUtil, csvUtil}
import org.apache.spark.sql.{SaveMode, SQLContext}
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

/**
  * @author
  */
class scatterGraphMatrix(taskId: String) extends BaseDao(taskId) {

  def exechistogram(sc: SparkContext, basePath: String, inputPath: String,
                    factorCol: String, featuresCol: String,
                    outputPath: String, taskId: String): Unit = {

    ///////////构建yarn-spark,从parquet中读取文件
    val sqlContext = new SQLContext(sc)
    val dataFrame = sqlContext.read.parquet(basePath + inputPath)
    //        val dataFrame = csvUtil.readCsv(sc, sqlContext, basePath + inputPath)
    viewDf(dataFrame, "dataFrame")
    ////////////////////将算法作用于数据集
    val cols = featuresCol.split(",")

    implicit lazy val formats = Serialization.formats(NoTypeHints)
    val colAndType =
      if ("" != factorCol)
        cols.map(colName => SummaryVO(colName, "num")) :+ SummaryVO(factorCol, "string")
      else
      //因子变量为空时返回的json不需要拼接到最后的json中
        cols.map(colName => SummaryVO(colName, "num"))
    val json = Serialization.write(colAndType)
    logger.info(json)
    ////////////////////保存结果及保存json
    //写入数据
    val finalDf = dataFrame.select("rownum",cols:+factorCol:_*)
    finalDf.write.format("parquet").mode(SaveMode.Overwrite).save(basePath + outputPath)
    super.flagSparked(taskId.toInt, outputPath, handleUtil.getHeadContent(finalDf.dtypes), json)
  }

  case class SummaryVO(colName: String, colType: String)

}

object scatterGraphMatrix {

  def main(args: Array[String]): Unit = {
    val basePath = ""
    val inputPath = "data/tree.csv"

    val factorCol = "col3"
    val featuresCol = "col1,col2"


    val outputPath = ""
    val taskId = "182"

    exec(handleUtil.getContext("scatterGraphMatrix"), args)

  }

  def exec(sc: SparkContext, args: Array[String]): Unit = {
    implicit lazy val formats = org.json4s.jackson.Serialization.formats(org.json4s.NoTypeHints)
    case class ArgVO(inputPath: String,
                    factorCol: String, featuresCol: String,
                    outputPath: String)

    val executer = new scatterGraphMatrix(args.last)
    try {
      args.foreach { x => executer.logger.info("inArg-" + x) }
      val basePath = args(0)
      val ArgVO(inputPath: String,
                    factorCol: String, featuresCol: String,
                    outputPath: String)
      = org.json4s.jackson.Serialization.read[ArgVO](args(1))
      val taskId = args(2)
      //executer.checkArgsNum(6, args.length)
      executer.exechistogram(sc: SparkContext, basePath: String, inputPath: String,
                    factorCol: String, featuresCol: String,
                    outputPath: String, taskId: String)

    } catch {
      case ex: Throwable => executer.handleException(args(args.length - 1).toInt, ex, "ScatterGraphMatrix")
    }
  }

}
