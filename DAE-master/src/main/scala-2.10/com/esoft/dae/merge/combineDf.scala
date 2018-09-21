package com.esoft.dae.merge

import com.esoft.dae.dao.BaseDao
import org.apache.spark.{SparkConf, SparkContext}
import com.esoft.dae.util.{handleUtil, csvUtil}
import org.apache.spark.sql.{SaveMode, SQLContext}

/**
  * @author
  */
class combineDf(taskId: String) extends BaseDao(taskId) {
  def execcombineDf(sc: SparkContext, basePath: String,
                    dfLeftCols: String, inputPath: String, dfRightCols: String, rightInputPath: String,
                    outputPath: String, taskId: String): Unit = {
    ////把两个Df的多列进行合并
    //    ///////////构建yarn-spark,从parquet中读取文件
    val sqlContext = new SQLContext(sc)
    val dfLeft = sqlContext.read.parquet(basePath + inputPath)
    //    val dfLeft = csvUtil.readCsv(sc, sqlContext, basePath + dfLeftPath)
    val dfRight = sqlContext.read.parquet(basePath + rightInputPath)
    //    val dfRight = csvUtil.readCsv(sc, sqlContext, basePath + dfRightPath)
    viewDf(dfLeft)
    viewDf(dfRight)
    //////////////////参数处理\
    assert(dfLeftCols != "", "数据源1未选择列")
    assert(dfRightCols != "", "数据源2未选择列")
    assert(dfLeft.count == dfRight.count,
      "数据源1和数据源2的行数不一样(" + dfLeft.count + ":" + dfRight.count + ")，不能进行列的合并")
    //获得最后df的那些列要放到最后结果中，并且重命名成什么列名
    val dfLeftTwoArr = handleUtil.strToTwoArr(dfLeftCols) //([a1,a2],[a3,a4])
    val dfLeftOldCols = "rownum" +: dfLeftTwoArr._1
    val dfLeftNewCols = "rownum" +: dfLeftTwoArr._2
    val dfUseLeft = dfLeft.select(dfLeftOldCols.head, dfLeftOldCols.tail: _*).toDF(dfLeftNewCols: _*)

    val dfRightTwoArr = handleUtil.strToTwoArr(dfRightCols)
    val dfRightOldCols = "rownum" +: dfRightTwoArr._1
    val dfRightNewCols = "rownum" +: dfRightTwoArr._2
    val dfUseRight = dfRight.select(dfRightOldCols.head, dfRightOldCols.tail: _*).toDF(dfRightNewCols: _*)

    val finalDf = dfUseLeft.join(dfUseRight, "rownum")
    viewDf(finalDf)
    ////////////////////保存结果及保存json
    finalDf.write.format("parquet")
      .mode(SaveMode.Overwrite)
      .save(basePath + outputPath)
    super.flagSparked(taskId.toInt, outputPath, handleUtil.getHeadContent(finalDf.dtypes), "no json")
  }

}

object combineDf {

  def main(args: Array[String]): Unit = {
    val basePath = ""

    val dfLeftCols = "a1 a3,a2 a4"
    val inputPath = "data/dfLeft.csv"
    val dfRightCols = "b1 b3,b2 b4"
    val rightInputPath = "data/dfRight.csv"

    val outputPath = ""
    val taskId = "182"

    //    val args = Array(basePath, inputPath,
    //      dfLeftCols, dfLeftPath, dfRightCols, dfRightPath,
    //      outputPath, taskId: String)
    //    val args = csvUtil.handleArg()
    //    val sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local"))
    //    exec(sc, args)

    exec(handleUtil.getContext("combineDf"), args)

  }

  def exec(sc: SparkContext, args: Array[String]): Unit = {
    implicit lazy val formats = org.json4s.jackson.Serialization.formats(org.json4s.NoTypeHints)
    case class ArgVO(featuresColLeft: String, inputPath: String,
                     featuresColRight: String, rightInputPath: String,
                     outputPath: String)

    val executer = new combineDf(args.last)
    try {
      args.foreach { x => executer.logger.info("inArg-" + x) }
      val basePath = args(0)
      val ArgVO(dfLeftCols: String, inputPath: String, dfRightCols: String, rightInputPath: String,
      outputPath: String)
      = org.json4s.jackson.Serialization.read[ArgVO](args(1))
      val taskId = args(2)
      //executer.checkArgsNum(8, args.length)
      executer.execcombineDf(sc: SparkContext, basePath: String,
        dfLeftCols: String, inputPath: String, dfRightCols: String, rightInputPath: String,
        outputPath: String, taskId: String)


    } catch {
      case ex: Throwable => executer.handleException(args(args.length - 1).toInt, ex, "lineREval")
    }
  }

}
