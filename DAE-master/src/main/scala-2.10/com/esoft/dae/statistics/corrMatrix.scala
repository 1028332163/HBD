package com.esoft.dae.statistics

import com.esoft.dae.dao.BaseDao
import org.apache.spark.{SparkConf, SparkContext}
import com.esoft.dae.util.{handleUtil, csvUtil}
import org.apache.spark.sql.SQLContext

/**
  * @author
  */
class corrMatrix(taskId: String) extends BaseDao(taskId) {
  def execDataFrameStatFunctions(sc: SparkContext, basePath: String, inputPath: String,
                                 corrType: String, featuresCol: String,
                                 outputPath: String, taskId: String): Unit = {
    ///////////构建yarn-spark,从parquet中读取文件
    val sqlContext = new SQLContext(sc)
    val dataFrame = sqlContext.read.parquet(basePath + inputPath)
    //        val dataFrame = csvUtil.readIntCsv(sc, sqlContext, basePath + inputPath)
    viewDf(dataFrame, "dataFrame")
    ////////////////////获得相关系数矩阵
    val stat = dataFrame.stat
    val cols = featuresCol.split(",")
    val size = cols.length
    val corrMatrix = new Array[Array[String]](size)
    val nf = java.text.NumberFormat.getInstance()
    nf.setGroupingUsed(false)
    nf.setMaximumFractionDigits(3)
    //    nf.format(stat.corr(cols(row), cols(col), corrType))
    for (row <- cols.indices) {
      val oneRow = for (col <- cols.indices) yield {
        col match {
          case index if index == row => 1.0.toString
          case index if index > row =>
            val corr = stat.corr(cols(row), cols(col), corrType)
            if (Double.NaN.equals(corr))
              "NaN"
            else
              nf.format(corr)
          case index if index < row => corrMatrix(col)(row) //根据上三角计算
        }
      }
      corrMatrix(row) = oneRow.toArray
    }
    corrMatrix.foreach(a => logger.info(a.mkString("", ",", "\n")))
    ////////////////////保存结果及保存json
    val json = handleUtil.getSymMatrixJson(cols, corrMatrix)
    logger.info(json)
    super.flagSparked(taskId.toInt, outputPath, "no head", json)
  }

}

object corrMatrix {

  def main(args: Array[String]): Unit = {
    val basePath = ""
    val inputPath = "data/tree.csv"

    val corrType = "pearson"
    val featuresCol = "col1,col2,label"


    val outputPath = ""
    val taskId = "182"

    //        val args = Array(basePath, inputPath,
    //          corrType,featuresCol,
    //          outputPath, taskId: String)
    //    val args = csvUtil.handleArg()
    //    val sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local"))
    //    exec(sc, args)

    exec(handleUtil.getContext("corrMatrix"), args)

  }

  def exec(sc: SparkContext, args: Array[String]): Unit = {
    implicit lazy val formats = org.json4s.jackson.Serialization.formats(org.json4s.NoTypeHints)
    case class ArgVO(inputPath: String,
                     corrType: String, featuresCol: String,
                     outputPath: String)

    val executer = new corrMatrix(args.last)
    try {
      args.foreach { x => executer.logger.info("inArg-" + x) }
      val basePath = args(0)
      val ArgVO(inputPath: String,
      corrType: String, featuresCol: String,
      outputPath: String)
      = org.json4s.jackson.Serialization.read[ArgVO](args(1))
      val taskId = args(2)
      //executer.checkArgsNum(6, args.length)
      executer.execDataFrameStatFunctions(sc: SparkContext, basePath: String, inputPath: String,
        corrType: String, featuresCol: String,
        outputPath: String, taskId: String)


    } catch {
      case ex: Throwable => executer.handleException(args(args.length - 1).toInt, ex, "corrMatrix")
    }
  }

}
