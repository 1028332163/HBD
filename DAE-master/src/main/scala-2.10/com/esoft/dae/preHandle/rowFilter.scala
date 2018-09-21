package com.esoft.dae.preHandle

import com.esoft.dae.dao.BaseDao
import org.apache.spark.{SparkConf, SparkContext}
import com.esoft.dae.util.{handleUtil, csvUtil}
import org.apache.spark.sql.{SaveMode, SQLContext}
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization._

/**
  * @author liuzw
  */
class rowFilter(taskId: String) extends BaseDao(taskId) {
  def execRowFilter(sc: SparkContext, basePath: String, inputPath: String,
                    colNames: String, conditionExpr: String,
                    outputPath: String, taskId: String): Unit = {
    ///////////构建yarn-spark,从parquet中读取文件
    val sqlContext = new SQLContext(sc)
    val dataFrame = sqlContext.read.parquet(basePath + inputPath)
    //    val dataFrame = csvUtil.readCsv(sc, sqlContext, basePath + inputPath)

    logger.info("oriDf-" + taskId + "-" + dataFrame.first().mkString("--"))
    //////////////////参数处理
    val (oriCols, newCols) =
      if ("" == colNames)
        (dataFrame.columns, dataFrame.columns)
      else
        handleUtil.strToTwoArr(colNames)
    val finalTmpDf =
      if ("" == conditionExpr) {
        val selectedDf = dataFrame.select(oriCols.head, oriCols.tail: _*)
        selectedDf.toDF(newCols: _*)
      }
      else {
        try {
          dataFrame.where(conditionExpr).select(oriCols.head, oriCols.tail: _*).toDF(newCols: _*)
        } catch {
          case ex: Throwable => throw new Exception("sql语句语法错误或spark不支持的语句")
        }
      }
    logger.info("finalTmpDf-" + taskId + "-" + finalTmpDf.first().mkString("--"))
    val finalDf = handleUtil.indexDataFrame(finalTmpDf)
    logger.info("finalDf-" + taskId + "-" + finalDf.first().mkString("--"))
    finalDf.show

    for (i <- Range(0, 10)) {
      logger.error("log" + i + "\n" + "this is new line" + "\n" + "this is second line" + "\n" + "this is third line")
      Thread.sleep(6000)
    }
    ////////////////////保存结果及保存json

    val oriRow = dataFrame.count()
    val newRow = finalDf.count()
    val oriCol = dataFrame.columns.length
    val newCol = finalDf.columns.length
    implicit lazy val formats = Serialization.formats(NoTypeHints)
    val json = "[" +
      write(ShowVO("result", oriRow, newRow, oriRow - newRow, oriCol, newCol, oriCol - newCol)) + "]"
    println(json)
    finalDf.write.format("parquet")
      .mode(SaveMode.Overwrite)
      .save(basePath + outputPath)
    super.flagSparked(taskId.toInt, outputPath, handleUtil.getHeadContent(finalDf.dtypes), json)
  }

  case class ShowVO(summary: String, oriRow: Double, newRow: Double, throwRow: Double,
                    oriCol: Double, newCol: Double, throwCol: Double)

}

object rowFilter {

  def main(args: Array[String]): Unit = {
    val basePath = ""
    val inputPath = "data/tree.csv"

    val colNames = "col1 col3,col2 col4"
    val conditionExpr = "col1>2 AND col2>2"

    val outputPath = ""
    val taskId = "182"

    //    val args = Array(basePath, inputPath,
    //      colNames, conditionExpr,
    //      outputPath, taskId: String)
    //    val args = csvUtil.handleArg()
    //    val sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local"))
    //    exec(sc, args)

    exec(handleUtil.getContext("rowFilter"), args)

  }


  def exec(sc: SparkContext, args: Array[String]): Unit = {
    implicit lazy val formats = org.json4s.jackson.Serialization.formats(org.json4s.NoTypeHints)
    case class ArgVO(inputPath: String,
                     featuresCol: String, conditionExpr: String,
                    outputPath: String)

    val executer = new rowFilter(args.last)
    try {
      args.foreach { x => executer.logger.info("inArg-" + x) }
      val basePath = args(0)
      val ArgVO(inputPath: String,
                    colNames: String, conditionExpr: String,
                    outputPath: String)
      = org.json4s.jackson.Serialization.read[ArgVO](args(1))
      val taskId = args(2)
      //executer.checkArgsNum(6, args.length)
      executer.execRowFilter(sc: SparkContext, basePath: String, inputPath: String,
                    colNames: String, conditionExpr: String,
                    outputPath: String, taskId: String)


    } catch {
      case ex: Throwable => executer.handleException(args(args.length - 1).toInt, ex, "rowFilter")
    }
  }

}
