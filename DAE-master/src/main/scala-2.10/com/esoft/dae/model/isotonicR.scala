package com.esoft.dae.model

import com.esoft.dae.dao.BaseDao
import com.esoft.dae.util.{ConstantInfo, handleUtil, csvUtil}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.{VectorUDT, Vectors}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.{Row, SaveMode, SQLContext}
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization._
import org.json4s.jackson.Serialization
import org.apache.spark.ml.regression.{IsotonicRegressionModel, IsotonicRegression}

/**
  * @author
  */
class isotonicR(taskId: String) extends BaseDao(taskId) {
  def execIsotonicRegression(sc: SparkContext, basePath: String, inputPath: String,
                             featuresCol: String, isotonic: String, labelCol: String, alPath: String,
                             predictionCol: String, weightCol: String,
                             outputPath: String, taskId: String): Unit = {
    /////////构建yarn-spark,从parquet中读取文件
    val sqlContext = new SQLContext(sc)
    val dataFrame = sqlContext.read.parquet(basePath + inputPath)
    viewDf(dataFrame, "dataFrame")
    //        //////////构建local-spark,从csv中读取文件
    //        val conf = new SparkConf().setAppName("test").setMaster("local")
    //        val sc = new SparkContext(conf)
    //        val sqlContext = new SQLContext(sc)
    //        val dataFrame = csvUtil.readCsv(sc, sqlContext, basePath + inputPath)
    //        viewDf(dataFrame,"dataFrame")
    //////////////////参数处理
    val modelSavePath = ConstantInfo.preModelPath + alPath
    val dfToUse = dataFrame
    ////////////////////将算法作用于数据集
    val isotonicRegression = new IsotonicRegression()
      .setFeaturesCol(featuresCol)
      .setIsotonic(isotonic.toBoolean)
      .setLabelCol(labelCol)
      .setPredictionCol(predictionCol)
    if (weightCol != "") {
      isotonicRegression.setWeightCol(weightCol)
    }
    val fitModel: IsotonicRegressionModel = isotonicRegression.fit(dfToUse)
    val finalDf = fitModel.transform(dfToUse)
    finalDf.show()

    ////////////////////页面展现用的df
    val boundaries = fitModel.boundaries
    val predictions = fitModel.predictions
    val showArr = for (i <- Range(0, boundaries.size)) yield Row(boundaries(i), predictions(i))
    val shoeSchema = handleUtil.getSchema("boundary double,prediction double")
    val showDf = sqlContext.createDataFrame(sc.parallelize(showArr), shoeSchema)
    showDf.show
    ////////////////////保存结果及保存json
    //保存showdf
    showDf.write.format("parquet")
      .mode(SaveMode.Overwrite)
      .save(basePath + outputPath)
    //保存模型
    fitModel.write.overwrite().save(basePath + modelSavePath)
    //    //更新数据库
    super.flagSparked(taskId.toInt, outputPath, "boundary double,prediction double", "no json")

  }

}

//    val test = sqlContext.createDataFrame(Seq(
//      (3.5, 1.0),
//      (4.0, 0.0),
//      (4.2, 0.0),
//      (5.0, 1.0),
//      (5.2, 0.0),
//      (6.0, 0.0),
//      (6.5, 0.0)
//    )).toDF("col2", "col1")
//    fitModel.transform(test).show
object isotonicR {

  def main(args: Array[String]): Unit = {
    val basePath = ""
    val inputPath = "data/test.csv"

    val featuresCol = "col2"
    val isotonic = "true" //true||false
    val labelCol = "label"
    val modelSavePath = "isotonicR/0815"
    val predictionCol = "prediction"
    val weightCol = ""

    val outputPath = "/out/20161206/p1549_1481002352573.parquet"
    val taskId = "182"


    //        val args = Array(basePath, inputPath,
    //          featuresCol, isotonic, labelCol, modelSavePath, predictionCol, weightCol,
    //          outputPath, taskId: String)
    exec(handleUtil.getContext("isotonicR"), args)

  }

  def exec(sc: SparkContext, args: Array[String]): Unit = {
    implicit lazy val formats = org.json4s.jackson.Serialization.formats(org.json4s.NoTypeHints)
    case class ArgVO(inputPath: String,
                             featuresCol: String, isotonic: String, labelCol: String, alPath: String,
                             predictionCol: String, weightCol: String,
                             outputPath: String)

    val executer = new isotonicR(args.last)
    try {
      args.foreach { x => executer.logger.info("inArg-" + x) }
      val basePath = args(0)
      val ArgVO(inputPath: String,
                             featuresCol: String, isotonic: String, labelCol: String, alPath: String,
                             predictionCol: String, weightCol: String,
                             outputPath: String)
      = org.json4s.jackson.Serialization.read[ArgVO](args(1))
      val taskId = args(2)
      //executer.checkArgsNum(10, args.length)
      executer.execIsotonicRegression(sc: SparkContext, basePath: String, inputPath: String,
                             featuresCol: String, isotonic: String, labelCol: String, alPath: String,
                             predictionCol: String, weightCol: String,
                             outputPath: String, taskId: String)


    } catch {
      case ex: Throwable => executer.handleException(args(args.length - 1).toInt, ex, "isotonicR")
    }
  }

}
