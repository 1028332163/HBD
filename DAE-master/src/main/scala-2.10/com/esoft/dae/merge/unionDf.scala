package com.esoft.dae.merge

import com.esoft.dae.dao.BaseDao
import org.apache.spark.{SparkConf, SparkContext}
import com.esoft.dae.util.{handleUtil, csvUtil}
import org.apache.spark.sql.{Row, SaveMode, SQLContext}

/**
  * @author
  */
class unionDf(taskId: String) extends BaseDao(taskId) {
  def execUnionDf(sc: SparkContext, basePath: String,
                  dfLeftCols: String, inputPath: String, dfLeftSql: String,
                  dfRightCols: String, rightInputPath: String, dfRightSql: String, dropDuplicates: String,
                  outputPath: String, taskId: String): Unit = {
    //    //    ///////////构建yarn-spark,从parquet中读取文件
    val sqlContext = new SQLContext(sc)
    val dfLeft = sqlContext.read.parquet(basePath + inputPath)
    val dfRight = sqlContext.read.parquet(basePath + rightInputPath)
    logger.info("dfLeft first row:" + dfLeft.first().toString())
    logger.info("dfRight first row:" + dfRight.first().toString())
    //    val dfLeft = csvUtil.readCsv(sc, sqlContext, basePath + dfLeftPath)
    //    dfLeft.show()
    //    val dfRight = csvUtil.readCsv(sc, sqlContext, basePath + dfRightPath)
    //    dfRight.show()
    //////////////////参数处理
    //把两个Df的行合并到一起
    val dfLeftTwoArr = handleUtil.strToTwoArr(dfLeftCols)
    val dfLeftOldCols = dfLeftTwoArr._1
    val dfLeftNewCols = dfLeftTwoArr._2
    val dfLeftToUse =
      if("".equals(dfLeftSql))
        dfLeft.select(dfLeftOldCols.head, dfLeftOldCols.tail: _*).toDF(dfLeftNewCols: _*)
      else
        dfLeft.where(dfLeftSql).select(dfLeftOldCols.head, dfLeftOldCols.tail: _*).toDF(dfLeftNewCols: _*)
    val dfRightTwoArr = handleUtil.strToTwoArr(dfRightCols)
    //L1->a L2->b =>((L1,L2),(a,b))  R1->b R2->a =>((R1,R2),(b,a))
    val dfRightColMap = Map(dfRightTwoArr._2.zip(dfRightTwoArr._1): _*) //map的形式是new-》old，为了col的顺序和dfLeft一致
    val dfRightUseCols = dfLeftNewCols.map(dfRightColMap(_))
    //unionall的api会直接将上下两个df堆叠，不会根据两个df的列名进行堆叠
    val unionDf =
      dfLeftToUse.unionAll(
        if ("".equals(dfRightSql))
          dfRight.select(dfRightUseCols.head, dfRightUseCols.tail: _*)
        else
          dfRight.where(dfRightSql).select(dfRightUseCols.head, dfRightUseCols.tail: _*)
      )
    val finalDf = handleUtil.indexDataFrame(if (dropDuplicates.toBoolean) unionDf.dropDuplicates() else unionDf)

    finalDf.show
    ////////////////////保存结果及保存json
    finalDf.write.format("parquet")
      .mode(SaveMode.Overwrite)
      .save(basePath + outputPath)
    super.flagSparked(taskId.toInt, outputPath, handleUtil.getHeadContent(finalDf.dtypes), "no json")
  }
}

object unionDf {

  def main(args: Array[String]): Unit = {
    val basePath = ""

    val featuresColLeft = "a1 A,a2 B"
    val inputPath = "data/dfLeft.csv"
    val dfLeftSql = "a1 > 1" //默认传true
    val featuresColRight = "b1 A,b2 B"
    val rightInputPath = "data/dfRight.csv"
    val dfRightSql = "b1 > 1" //默认传true
    val dropDuplicates = "true"

    val outputPath = ""
    val taskId = "182"

    //    val args = Array(basePath, inputPath,
    //      dfLeftCols, dfLeftPath, dfLeftSql, dfRightCols, dfRightPath, dfRightSql, dropDuplicates,
    //      outputPath, taskId: String)
    //    val sc = new SparkContext(new SparkConf().setAppName("test").setAppName("local"))
    exec(handleUtil.getContext("unionDf"), args)

  }


  def exec(sc: SparkContext, args: Array[String]): Unit = {
    implicit lazy val formats = org.json4s.jackson.Serialization.formats(org.json4s.NoTypeHints)
    case class ArgVO(featuresColLeft: String, inputPath: String, dfLeftSql: String,
                     featuresColRight: String, rightInputPath: String, dfRightSql: String, dropDuplicates: String,
                     outputPath: String)

    val executer = new unionDf(args.last)
    try {
      args.foreach { x => executer.logger.info("inArg-" + x) }
      val basePath = args(0)
      val ArgVO(dfLeftCols: String, inputPath: String, dfLeftSql: String,
      dfRightCols: String, rightInputPath: String, dfRightSql: String, dropDuplicates: String,
      outputPath: String)
      = org.json4s.jackson.Serialization.read[ArgVO](args(1))
      val taskId = args(2)
      //executer.checkArgsNum(11, args.length)
      executer.execUnionDf(sc: SparkContext, basePath: String,
        dfLeftCols: String, inputPath: String, dfLeftSql: String,
        dfRightCols: String, rightInputPath: String, dfRightSql: String, dropDuplicates: String,
        outputPath: String, taskId: String)
    } catch {
      case ex: Throwable => executer.handleException(args(args.length - 1).toInt, ex, "unionDf")
    }
  }

}
