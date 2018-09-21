package com.esoft.dae.preHandle

import com.esoft.dae.dao.BaseDao
import com.esoft.dae.util.handleUtil
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{SQLContext, SaveMode}

/**
  * @author
  */
class randomSample(taskId: String) extends BaseDao(taskId) {
  def execRandomSample(sc: SparkContext, basePath: String, inputPath: String,
                       fractionNum: String, fractionPro: String, seed: String, withReplacement: String,
                       outputPath: String, taskId: String): Unit = {
    ///////////构建yarn-spark,从parquet中读取文件
    val sqlContext = new SQLContext(sc)
    val dataFrame = sqlContext.read.parquet(basePath + inputPath)
    //    val dataFrame = csvUtil.readCsv(sc, sqlContext, basePath + inputPath)
    viewDf(dataFrame, "dataFrame")
    //////////////////参数处理
    assert(fractionPro != "" || fractionNum != "", "抽样比率和抽样个数都未进行选择")
    val fraction =
    //需要一个比例数
      if ("" == fractionPro)
        fractionNum.toDouble / dataFrame.count
      else
        fractionPro.toDouble
    assert(fraction >= 0, "抽样的比率需要大于等于0")
    if (!withReplacement.toBoolean) {
      if ("" == fractionPro) {
        //使用采样个数
        assert(fraction <= 1, "不放回抽样的最终样本数需要小于数据集的本身大小")
      } else {
        //使用采样比率
        assert(fraction <= 1, "不放回抽样的比率需要小于等于1")
      }
    }
    println("fraction:" + fraction.toDouble)
    val tmpFinalDf =
      if ("" == seed)
        dataFrame.sample(withReplacement.toBoolean, fraction.toDouble)
      else
        dataFrame.sample(withReplacement.toBoolean, fraction.toDouble, seed.toLong)
    val finalDf = handleUtil.indexDataFrame(tmpFinalDf.drop("rownum"))
    finalDf.show
    println(finalDf.count)
    ////////////////////保存结果及保存json
    finalDf.write.format("parquet")
      .mode(SaveMode.Overwrite)
      .save(basePath + outputPath)
    super.flagSparked(taskId.toInt, outputPath, handleUtil.getHeadContent(finalDf.dtypes), "no json")
  }

}

object randomSample {

  def main(args: Array[String]): Unit = {
    val basePath = ""
    val inputPath = "data/tree.csv"

    val fractionNum = "5"
    val fractionPro = "-1" //每个样本被抽中的概率，所以最终的样本数会在size*fractionPro值附近波动
    val withReplacement = "true"
    val seed = ""


    val outputPath = ""
    val taskId = "182"

    //    val args = Array(basePath, inputPath,
    //      fractionNum, fractionPro, seed, withReplacement,
    //      outputPath, taskId: String)

    exec(handleUtil.getContext("randomSample"), args)

  }

  //////sample 的详细解释在rdd里面
  def exec(sc: SparkContext, args: Array[String]): Unit = {
    implicit lazy val formats = org.json4s.jackson.Serialization.formats(org.json4s.NoTypeHints)
    case class ArgVO(inputPath: String,
                       fractionNum: String, fractionPro: String, seed: String, withReplacement: String,
                       outputPath: String)

    val executer = new randomSample(args.last)
    try {
      args.foreach { x => executer.logger.info("inArg-" + x) }
      val basePath = args(0)
      val ArgVO(inputPath: String,
                       fractionNum: String, fractionPro: String, seed: String, withReplacement: String,
                       outputPath: String)
      = org.json4s.jackson.Serialization.read[ArgVO](args(1))
      val taskId = args(2)
      //executer.checkArgsNum(8, args.length)
      executer.execRandomSample(sc: SparkContext, basePath: String, inputPath: String,
                       fractionNum: String, fractionPro: String, seed: String, withReplacement: String,
                       outputPath: String, taskId: String)


    } catch {
      case ex: Throwable => executer.handleException(args(args.length - 1).toInt, ex, "randomSample")
    }
  }

}
