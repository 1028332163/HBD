package com.esoft.dae.statistics

import com.esoft.dae.dao.BaseDao
import com.esoft.dae.util.handleUtil
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.{Accumulator, SparkContext, SparkConf}
import org.apache.spark.sql.{DataFrame, SQLContext, functions}
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

/**
  * @ liuzw
  */
class colsSta(taskId: String) extends BaseDao(taskId) {
  def execDfShow(sc: SparkContext, basePath: String, inputPath: String,
                 featuresCol: String,
                 outputPath: String, taskId: String): Unit = {
    ///////构建yarn-spark,从parquet中读取文件
    val sqlContext = new SQLContext(sc)
    val dataFrame = sqlContext.read.parquet(basePath + inputPath)
    viewDf(dataFrame, "dataFrame")
    //////////////////参数处理
    assert(featuresCol != "", "未选择特征列")
    val rowNum = dataFrame.count().toString
    val nameToString = dataFrame.dtypes.toMap
    val stringIndexer = new StringIndexer().setHandleInvalid("skip")
    val colInfoVoArr = featuresCol.split(",").map {
      colName =>
        val colType = nameToString(colName)
        val dfToUse = dataFrame.select(colName)
        val (naCount, nullCount) = getCount(colName, dfToUse, sc)
        val perfectDf = dfToUse.na.drop(Array(colName))
        val (min, max, mean, std, factorNum) =
          if (colType == "DoubleType" || colType == "IntegerType") {
            //数字类型
            (
              perfectDf.select(functions.min(colName)).first.get(0).toString,
              perfectDf.select(functions.max(colName)).first.get(0).toString,
              perfectDf.select(functions.mean(colName)).first.get(0).toString,
              perfectDf.select(functions.stddev(colName)).first.get(0).toString,
              "_"
              )
          } else {
            ("-", "-", "-", "-",
              stringIndexer.setInputCol(colName).fit(perfectDf).labels.length.toString
              )
          }

        ColInfoVo(colName, colType, rowNum, naCount, nullCount, min, max, mean, std, factorNum)
    }

    implicit lazy val formats = Serialization.formats(NoTypeHints)
    val json = Serialization.write(colInfoVoArr)
    println(json)
    ////////////////////保存结果及保存jsons
    super.flagSparked(taskId.toInt, outputPath, "no head", json)
  }

  //统计dataFrame中的colName列的na和null的个数
  def getCount(colName: String, dataFrame: DataFrame, sc: SparkContext): (String, String) = {
    val naCount: Accumulator[Int] = sc.accumulator(0, "naCount") //统计NaN的个数
    val nullCount = sc.accumulator(0, "nullCount") //统计null的个数
    dataFrame.foreach { row =>
      row(0) match {
        case j if Double.NaN.equals(j) => naCount += 1 //equals可以，==不可以
        case j if null == j => nullCount += 1
        case _ =>
      }
    }
    (naCount.value.toString, nullCount.value.toString)
  }

  case class ColInfoVo(colName: String, colType: String, rowNum: String, naNum: String, nullNum: String,
                       min: String, max: String, mean: String, std: String, factorNum: String)

}

object colsSta {

  def main(args: Array[String]): Unit = {
    val basePath = ""
    val inputPath = "data/test.csv"

    val featuresCol = "col1,col2"

    val outputPath = ""
    val taskId = "182"

    //    val args = Array(basePath, inputPath,
    //      featuresCol,
    //      outputPath, taskId: String)
    exec(handleUtil.getContext("colsSta"), args)

  }

  def exec(sc: SparkContext, args: Array[String]): Unit = {
    implicit lazy val formats = org.json4s.jackson.Serialization.formats(org.json4s.NoTypeHints)
    case class ArgVO(inputPath: String,
                     featuresCol: String,
                 outputPath: String)

    val executer = new colsSta(args.last)
    try {
      args.foreach { x => executer.logger.info("inArg-" + x) }
      val basePath = args(0)
      val ArgVO(inputPath: String,
                 featuresCol: String,
                 outputPath: String)
      = org.json4s.jackson.Serialization.read[ArgVO](args(1))
      val taskId = args(2)
      //executer.checkArgsNum(5, args.length)
      executer.execDfShow(sc: SparkContext, basePath: String, inputPath: String,
                 featuresCol: String,
                 outputPath: String, taskId: String)


    } catch {
      case ex: Throwable => executer.handleException(args(args.length - 1).toInt, ex, "chiSquareTest")
    }
  }

}
