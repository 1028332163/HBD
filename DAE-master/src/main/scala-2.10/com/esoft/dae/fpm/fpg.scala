package com.esoft.dae.fpm

import com.esoft.dae.dao.BaseDao
import org.apache.spark.mllib.fpm.AssociationRules.Rule
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import com.esoft.dae.util.{handleUtil, csvUtil}
import org.apache.spark.sql.{Row, SaveMode, SQLContext}
import org.apache.spark.mllib.fpm.FPGrowth

/**
  * @author liuzw
  */
class fpg(taskId: String) extends BaseDao(taskId) {
  def execFPGrowth(sc: SparkContext, basePath: String, inputPath: String,
                   itemsCol: String, itemsColSep: String, minConfidence: String, minSupport: String,
                   outputPath: String, taskId: String): Unit = {
    /////////构建yarn-spark,从parquet中读取文件
    val sqlContext = new SQLContext(sc)
    val dataFrame = sqlContext.read.parquet(basePath + inputPath)
    //        val dataFrame = csvUtil.readStrCsv(sc, sqlContext, basePath + inputPath)
    viewDf(dataFrame)
    //////////////////参数处理
    val transactions: RDD[Array[String]] = dataFrame.select(itemsCol).rdd
      .map {
        row =>
          try {
            row(0).toString.split(itemsColSep).toSet.toArray
          } catch {
            case ex: Throwable => Array("_")
          }
      }
    logger.info("first transaction:" + transactions.first().mkString(","))
    ////////////////////将算法作用于数据集
    val fPGrowth = new FPGrowth()
    val model = fPGrowth.run(transactions)
    val rules: RDD[Rule[String]] = model.generateAssociationRules(minConfidence.toDouble)
    val showRdd: RDD[Row] = rules.map(rule =>
      Row.fromSeq(Array(rule.antecedent.mkString("[", ",", "]"),
        rule.consequent.mkString("[", ",", "]"),
        rule.confidence)))


    val showDf = handleUtil.indexDataFrame(sqlContext.createDataFrame(showRdd,
      handleUtil.getSchema("antecedent string,consequent string,confidence double")))
    showDf.show()

    val finalRdd = sc.parallelize(Array(Row.fromSeq(Array("totalItemCount", -dataFrame.count.toDouble)))) ++
      model.freqItemsets.map { itemset =>
        Row.fromSeq(Array(itemset.items.mkString("[", itemsColSep, "]"), -itemset.freq.toDouble))
      }
    ///df无法实现降序排列，需先使用freq的负数升序排列，再将结果df转化
    val finalTmpDf =
      sqlContext.createDataFrame(
        sqlContext.createDataFrame(finalRdd, handleUtil.getSchema("itemset string,freq double")).orderBy("freq")
          .map(row => Row(row(0), -row.getDouble(1))), handleUtil.getSchema("itemset string,freq double"))
    val finalDf = handleUtil.indexDataFrame(finalTmpDf)
    finalDf.show
    ////////////////////保存结果及保存json
    val showDfPath = handleUtil.getDfShowPath(outputPath) + "1.parquet"
    showDf.write.format("parquet")
      .mode(SaveMode.Overwrite)
      .save(basePath + showDfPath)
    updateTaskInsData(taskId.toInt, showDfPath, showDf.dtypes, ONLY_ONE_TABLE, "no json")

    finalDf.write.format("parquet")
      .mode(SaveMode.Overwrite)
      .save(basePath + outputPath)
    super.flagSparked(taskId.toInt, outputPath, handleUtil.getHeadContent(finalDf.dtypes), "no json")
  }
}

object fpg {

  def main(args: Array[String]): Unit = {
    val basePath = ""
    val inputPath = "data/items.csv"

    val itemsCol = "col1" //项集列名 单选
    val itemsColSep = "_" //项集列分隔符
    val minSupport = "0.3" //最小支持度 [0,1]
    val minConfidence = "0.8" //最小置信度 [0,1]



    val outputPath = "out/fpg"
    val taskId = "182"


    //        val args = Array(basePath: String, inputPath: String,
    //          itemsCol: String, itemsColSep: String, minConfidence: String, minSupport: String,
    //          outputPath: String, taskId: String)
    //        val args = csvUtil.handleArg()
    //        val sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local"))
    //        exec(sc, args)

    exec(handleUtil.getContext("fpg"), args)

  }


  def exec(sc: SparkContext, args: Array[String]): Unit = {
    implicit lazy val formats = org.json4s.jackson.Serialization.formats(org.json4s.NoTypeHints)
    case class ArgVO(inputPath: String,
                   itemsCol: String, itemsColSep: String, minConfidence: String, minSupport: String,
                   outputPath: String)

    val executer = new fpg(args.last)
    try {
      args.foreach { x => executer.logger.info("inArg-" + x) }
      val basePath = args(0)
      val ArgVO(inputPath: String,
                   itemsCol: String, itemsColSep: String, minConfidence: String, minSupport: String,
                   outputPath: String)
      = org.json4s.jackson.Serialization.read[ArgVO](args(1))
      val taskId = args(2)
      //executer.checkArgsNum(8, args.length)
      executer.execFPGrowth(sc: SparkContext, basePath: String, inputPath: String,
                   itemsCol: String, itemsColSep: String, minConfidence: String, minSupport: String,
                   outputPath: String, taskId: String)


    } catch {
      case ex: Throwable => executer.handleException(args(args.length - 1).toInt, ex, "fpg")
    }
  }

}
