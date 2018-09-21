package com.esoft.dae.fpm

import com.esoft.dae.dao.BaseDao
import org.apache.spark.mllib.fpm.AssociationRules.Rule
import org.apache.spark.mllib.fpm.FPGrowth.FreqItemset
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import com.esoft.dae.util.{handleUtil, csvUtil}
import org.apache.spark.sql.{Row, SaveMode, SQLContext}
import org.apache.spark.mllib.fpm.AssociationRules

/**
  * @author
  */
class associationRules(taskId: String) extends BaseDao(taskId) {
  def execAssociationRules(sc: SparkContext, basePath: String, inputPath: String,
                           freqCol: String, itemsCol: String, itemsColSep: String, minConfidence: String,
                           outputPath: String, taskId: String): Unit = {
    /////////构建yarn-spark,从parquet中读取文件
    val sqlContext = new SQLContext(sc)
    val dataFrame = sqlContext.read.parquet(basePath + inputPath)
    //    val dataFrame = csvUtil.readStrCsv(sc, sqlContext, basePath + inputPath)
    viewDf(dataFrame, "dataFrame")
    //////////////////参数处理
    val freqItemsets: RDD[FreqItemset[String]] = dataFrame.select(itemsCol, freqCol).rdd
      .filter { row => try {
        assert(row(0).toString.split(itemsColSep).toSet.toArray.length > 0)
        assert(row(1).toString.toDouble.toLong > 0)
        true
      } catch {
        case ex: Throwable => false
      }
      } //将无效的频繁项集和不合法的频数去掉
      .map(
      row => new
          FreqItemset(row(0).toString.split(itemsColSep).toSet.toArray, row(1).toString.toDouble.toLong)
    )
    ////////////////////将算法作用于数据集
    val rules: RDD[Rule[String]] = new AssociationRules().run(freqItemsets)
    val finalRdd: RDD[Row] = rules.map(rule =>
      Row.fromSeq(Array(rule.antecedent.mkString("[", ",", "]"),
        rule.consequent.mkString("[", ",", "]"),
        rule.confidence.toString)))
    val finalDf = handleUtil.indexDataFrame(sqlContext.createDataFrame(finalRdd, handleUtil.getSchema("antecedent string,consequent string,confidence string")))
    finalDf.show
    ////////////////////保存结果及保存json
    finalDf.write.format("parquet")
      .mode(SaveMode.Overwrite)
      .save(basePath + outputPath)
    super.flagSparked(taskId.toInt, outputPath, handleUtil.getHeadContent(finalDf.dtypes), "no json")
  }
}

object associationRules {

  def main(args: Array[String]): Unit = {
    val basePath = ""
    val inputPath = "data/freqItems.csv"

    val freqCol = "freq" //项集频数列
    val itemsCol = "items" //项集列名 单选
    val itemsColSep = "-" //项集列分隔符
    val minConfidence = "0.8" //最小置信度 [0,1]

    val outputPath = ""
    val taskId = "182"


    //    val args = Array(basePath, inputPath,
    //      freqCol, itemsCol, itemsColSep, minConfidence,
    //      outputPath, taskId: String)
    exec(handleUtil.getContext("associationRules"), args)

  }

  def exec(sc: SparkContext, args: Array[String]): Unit = {
    implicit lazy val formats = org.json4s.jackson.Serialization.formats(org.json4s.NoTypeHints)
    case class ArgVO(inputPath: String,
                           freqCol: String, itemsCol: String, itemsColSep: String, minConfidence: String,
                           outputPath: String)

    val executer = new associationRules(args.last)
    try {
      args.foreach { x => executer.logger.info("inArg-" + x) }
      val basePath = args(0)
      val ArgVO(inputPath: String,
                           freqCol: String, itemsCol: String, itemsColSep: String, minConfidence: String,
                           outputPath: String)
      = org.json4s.jackson.Serialization.read[ArgVO](args(1))
      val taskId = args(2)
      //executer.checkArgsNum(8, args.length)
      executer.execAssociationRules(sc: SparkContext, basePath: String, inputPath: String,
                           freqCol: String, itemsCol: String, itemsColSep: String, minConfidence: String,
                           outputPath: String, taskId: String)


    } catch {
      case ex: Throwable => executer.handleException(args(args.length - 1).toInt, ex, "associationRules")
    }
  }

}
