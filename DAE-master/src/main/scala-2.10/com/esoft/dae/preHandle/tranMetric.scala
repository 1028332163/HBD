package com.esoft.dae.preHandle

import com.esoft.dae.dao.BaseDao
import com.esoft.dae.util.{handleUtil, csvUtil}
import org.apache.spark.sql.{SaveMode, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}


class tranMetric(taskId: String) extends BaseDao(taskId) {
  def tranMetric(sc: SparkContext, basePath: String, inputPath: String,
                 colNamesPara: String, operation: String,
                 outputPath: String, taskId: String) {
    assert("" != colNamesPara, "没选择进行尺度变换的列名")
    val sqlContext = new SQLContext(sc)
    val dataFrame = sqlContext.read.parquet(basePath + inputPath)
    //    val dataFrame = csvUtil.readCsv(sc, sqlContext, basePath + inputPath)
    dataFrame.show()
    viewDf(dataFrame, "dataFrame")
    //////////////////////参数处理
    val colNames = colNamesPara.split(",")
    val dfCols = dataFrame.columns
    val tranColIndexs: IndexedSeq[Int] =
      for (col <- dfCols.indices if colNames.contains(dfCols(col))) yield col
    val finalRdd = dataFrame.map { row =>
      //      (1,1,1)
      //      [0,2]
      val newRow = for (i <- 0 until row.size) yield {
        if (tranColIndexs.contains(i)) {
          //负数进行log运算和sqrt会被计算成Double.Nan
          //需要进行变换的列
          try {
            if ("abs".equals(operation))
              Math.abs(row(i).toString.toDouble)
            else if ("sqrt".equals(operation))
              Math.sqrt(row(i).toString.toDouble)
            else if ("ln".equals(operation))
              Math.log(row(i).toString.toDouble)
            else if ("log10".equals(operation))
              Math.log10(row(i).toString.toDouble)
            else if ("log2".equals(operation))
              Math.log(row(i).toString.toDouble) / Math.log(2)
            else {
              row(i)
            }
          } catch {
            case ex: Throwable => null
          }
        } else {
          //不需要进行变换的列
          row(i)
        }
      }
      Row.fromSeq(newRow)
    }
    val finalDf = sqlContext.createDataFrame(finalRdd, dataFrame.schema)
    finalDf.show()
    //    写入数据
    finalDf.write.format("parquet").mode(SaveMode.Overwrite).save(basePath + outputPath)
    super.flagSparked(taskId.toInt, outputPath, handleUtil.getHeadContent(dataFrame.dtypes), "no json")
  }

}

object tranMetric {

  def main(args: Array[String]) {
    val basePath = ""
    val inputPath = "data/tree.csv"

    val colNames = "col1,col2"
    val operation = "sqrt" //ln,log2,log10,abs,sqrt


    val outputPath = "out/lzw_typeConvert_Out.parquet"
    val taskID = "12"

    //        val args = Array(basePath, inputPath, colNames, operation, head, outputPath, taskID)
    //        val args = csvUtil.handleArg()
    //        val sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local"))
    //        exec(sc, args)

    exec(handleUtil.getContext("tranMetric"), args)

  }

  def exec(sc: SparkContext, args: Array[String]): Unit = {
    implicit lazy val formats = org.json4s.jackson.Serialization.formats(org.json4s.NoTypeHints)
    case class ArgVO(inputPath: String,
                     featuresCol: String, operation: String,
                 outputPath: String)

    val executer = new tranMetric(args.last)
    try {
      args.foreach { x => executer.logger.info("inArg-" + x) }
      val basePath = args(0)
      val ArgVO(inputPath: String,
                 colNamesPara: String, operation: String,
                 outputPath: String)
      = org.json4s.jackson.Serialization.read[ArgVO](args(1))
      val taskId = args(2)
      //executer.checkArgsNum(7, args.length)
      executer.tranMetric(sc: SparkContext, basePath: String, inputPath: String,
                 colNamesPara: String, operation: String,
                 outputPath: String, taskId: String)
    } catch {
      case ex: Throwable => executer.handleException(args(args.length - 1).toInt, ex, "tranMetric")
    }
  }

}
