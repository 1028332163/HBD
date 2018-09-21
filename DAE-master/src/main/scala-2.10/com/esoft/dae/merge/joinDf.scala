package com.esoft.dae.merge

import com.esoft.dae.dao.BaseDao

import org.apache.spark.{SparkConf, SparkContext}
import com.esoft.dae.util.{handleUtil, csvUtil}
import org.apache.spark.sql.{Column, SaveMode, SQLContext}

/**
  * @author liuzw
  */
class joinDf(taskId: String) extends BaseDao(taskId) {
  def execJoin(sc: SparkContext, basePath: String,
               dfLeftCols: String, inputPath: String, dfRightCols: String, rightInputPath: String,
               joinExprs: String, joinType: String,
               outputPath: String, taskId: String): Unit = {
    ///////////构建yarn-spark,从parquet中读取文件
    val sqlContext = new SQLContext(sc)
    val dfLeftOri = sqlContext.read.parquet(basePath + inputPath)
    val dfRight = sqlContext.read.parquet(basePath + rightInputPath)
    dfLeftOri.show()
    dfRight.show()
    logger.info("dfLeft first row:" + dfLeftOri.first().toString())
    logger.info("dfRight first row:" + dfRight.first().toString())
    //////////构建local-spark,从csv中读取文件
    //            val sqlContext = new SQLContext(sc)
    //            val dfLeftOri = csvUtil.readCsv(sc, sqlContext, basePath + dfLeftPath)
    //        dfLeftOri.show()
    //            val dfRight = csvUtil.readCsv(sc, sqlContext, basePath + dfRightPath)
    //            dfRight.show()

    ////////////参数处理

    assert(joinExprs != "", "join表达式不能为空")
    assert(dfLeftCols != "", "数据源1未选择列")
    assert(dfRightCols != "", "数据源2未选择列")
    ///需要对列名进行重命名（左侧所有列名都加了uniFlag），要不导致join之后的dataframe会有同名的列，无法进行select
    val uniFlag = "_dfLeft"
    val leftCols = dfLeftOri.columns.map(_ + uniFlag)
    val dfLeft = dfLeftOri.toDF(leftCols: _*)
    val joinCols =
      joinExprs.split(",").map { couple =>
        val leftRight = couple.split(" ")
        (leftRight(0), leftRight(1))
      } //[(a1,b1),(a2,b2)]

    val columns =
      joinCols.map(tuple => dfLeft(tuple._1 + uniFlag) === dfRight(tuple._2)) //[a1==b1,a2==b2]
    val column: Column = columns.foldLeft(columns.head)((a: Column, b: Column) => a && b) //(a1==b1)&&(a2==b2)
    //    val column = dfLeft("col1") === dfRight("col1")

    //将两个dataFrame做join并且重命名
    val dfLeftInfo = handleUtil.strToTwoArr(dfLeftCols) //([a1,a2],[a3,a4])
    val dfRightInfo = handleUtil.strToTwoArr(dfRightCols)
    val (oriCols, newCols) =
      if ("leftsemi" == joinType)
        (dfLeftInfo._1.map(_ + uniFlag), dfLeftInfo._2)
      else
        (dfLeftInfo._1.map(_ + uniFlag) ++: dfRightInfo._1, dfLeftInfo._2 ++: dfRightInfo._2)
    ////////////////////将算法作用于数据集
    val finalDf = handleUtil.indexDataFrame(dfLeft.join(dfRight, column, joinType).select(oriCols.head, oriCols.tail: _*).toDF(newCols: _*))
    finalDf.show
    ////////////////////保存结果及保存json
    finalDf.write.format("parquet")
      .mode(SaveMode.Overwrite)
      .save(basePath + outputPath)
    super.flagSparked(taskId.toInt, outputPath, handleUtil.getHeadContent(finalDf.dtypes), "no json")
  }


}

object joinDf {

  def main(args: Array[String]): Unit = {
    val basePath = ""

    val dfLeftCols = "col1 a3,col2 a4"
    val inputPath = "data/tree.csv"
    val dfRightCols = "col1 b3,col2 b4"
    val rightInputPath = "data/tree.csv"
    val joinExprs = "col1 col1,col2 col2"
    val joinType = "leftsemi" //inner||outer||left_outer||right_outer||leftsemi

    val outputPath = ""
    val taskId = "182"

    //    val args = Array(basePath, inputPath,
    //      dfLeftCols, dfLeftPath, dfRightCols, dfRightPath, joinExprs, joinType,
    //      outputPath, taskId: String)
    //    //            val args = csvUtil.handleArg()
    //    val sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local"))
    //    exec(sc, args)

    exec(handleUtil.getContext("joinDf"), args)

  }

  def exec(sc: SparkContext, args: Array[String]): Unit = {
    implicit lazy val formats = org.json4s.jackson.Serialization.formats(org.json4s.NoTypeHints)
    case class ArgVO(featuresColLeft: String, inputPath: String,
                     featuresColRight: String, rightInputPath: String,
                     joinExprs: String, joinType: String, outputPath: String)

    val executer = new joinDf(args.last)
    try {
      args.foreach { x => executer.logger.info("inArg-" + x) }
      val basePath = args(0)
      val ArgVO(dfLeftCols: String, inputPath: String, dfRightCols: String, rightInputPath: String,
      joinExprs: String, joinType: String,
      outputPath: String)
      = org.json4s.jackson.Serialization.read[ArgVO](args(1))
      val taskId = args(2)
      //executer.checkArgsNum(10, args.length)
      executer.execJoin(sc: SparkContext, basePath: String,
        dfLeftCols: String, inputPath: String, dfRightCols: String, rightInputPath: String,
        joinExprs: String, joinType: String,
        outputPath: String, taskId: String)


    } catch {
      case ex: Throwable => executer.handleException(args(args.length - 1).toInt, ex, "lineREval")
    }
  }

}
