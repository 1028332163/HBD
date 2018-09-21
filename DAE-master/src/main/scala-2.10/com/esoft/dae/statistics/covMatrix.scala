package com.esoft.dae.statistics

import com.esoft.dae.dao.BaseDao
import org.apache.spark.{SparkConf, SparkContext}
import com.esoft.dae.util.{handleUtil, csvUtil}
import org.apache.spark.mllib.linalg.{VectorUDT, Vectors}
import org.apache.spark.sql.{DataFrameStatFunctions, SaveMode, SQLContext}
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization.write
import org.json4s.jackson.Serialization

/**
  * @author
  */
class covMatrix(taskId: String) extends BaseDao(taskId) {
  def execDataFrameStatFunctions(sc: SparkContext, basePath: String, inputPath: String,
                                 featuresCol: String,
                                 outputPath: String, taskId: String): Unit = {
    ///////////构建yarn-spark,从parquet中读取文件
    val sqlContext = new SQLContext(sc)
    val dataFrame = sqlContext.read.parquet(basePath + inputPath)
    //    val dataFrame = csvUtil.readCsv(sc, sqlContext, basePath + inputPath)
    viewDf(dataFrame, "dataFrame")
    ////////////////////将算法作用于数据集
    val stat: DataFrameStatFunctions = dataFrame.stat
    val cols = featuresCol.split(",")
    val size = cols.length
    val nf = java.text.NumberFormat.getInstance()
    nf.setGroupingUsed(false)
    nf.setMaximumFractionDigits(1)
    ///////计算协方差矩阵
    val covMatrix = new Array[Array[String]](size)
    for (row <- cols.indices) {
      val oneRow = for (col <- cols.indices) yield {
        col match {
          case index if index == row => 1.0.toString
          case index if index > row => nf.format(stat.cov(cols(row), cols(col))) //使用API计算协方差
          case index if index < row => covMatrix(col)(row) //根据上三角计算
        }
      }
      covMatrix(row) = oneRow.toArray
    }
    //向页面返回信息
    val json = handleUtil.getSymMatrixJson(cols, covMatrix)
    logger.info("finalJson:" + json)
    ////////////////////保存结果及保存json
    super.flagSparked(taskId.toInt, outputPath, "no head", json)
  }

}

object covMatrix {

  def main(args: Array[String]): Unit = {
    val basePath = ""
    val inputPath = "data/lineRegression.csv"

    val featuresCol = "col1,col2,label"

    val outputPath = ""
    val taskId = "182"

    //    val args = Array(basePath, inputPath, featuresCol, outputPath, taskId: String)
    //    val args = csvUtil.handleArg()
    //    val sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local"))
    //    exec(sc, args)

    exec(handleUtil.getContext("covMatrix"), args)

  }

  def exec(sc: SparkContext, args: Array[String]): Unit = {
    implicit lazy val formats = org.json4s.jackson.Serialization.formats(org.json4s.NoTypeHints)
    case class ArgVO(inputPath: String,
                     featuresCol: String,
                     outputPath: String)

    val executer = new covMatrix(args.last)
    try {
      args.foreach { x => executer.logger.info("inArg-" + x) }
      val basePath = args(0)
      val ArgVO(inputPath: String,
      featuresCol: String,
      outputPath: String)
      = org.json4s.jackson.Serialization.read[ArgVO](args(1))
      val taskId = args(2)
      //executer.checkArgsNum(5, args.length)
      executer.execDataFrameStatFunctions(sc: SparkContext, basePath: String, inputPath: String,
        featuresCol: String,
        outputPath: String, taskId: String)


    } catch {
      case ex: Throwable => executer.handleException(args(args.length - 1).toInt, ex, "covMatrix")
    }
  }

}
