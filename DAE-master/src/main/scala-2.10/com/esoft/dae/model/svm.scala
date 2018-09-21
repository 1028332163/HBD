package com.esoft.dae.model

import com.esoft.dae.dao.BaseDao
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.SparkContext
import com.esoft.dae.util.{ConstantInfo, handleUtil}
import org.apache.spark.sql.SQLContext
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization._
import org.json4s.jackson.Serialization
import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}

/**
  * @author
  */
class svm(taskId: String) extends BaseDao(taskId) {
  def execSVMWithSGD(sc: SparkContext, basePath: String, inputPath: String,
                     addIntercept: String, featuresCol: String, labelCol: String, predictionCol: String,alPath: String,
                     validateData: String, outputPath: String, taskId: String): Unit = {
    ///////////构建yarn-spark,从parquet中读取文件
    val sqlContext = new SQLContext(sc)
    val dataFrame = sqlContext.read.parquet(basePath + inputPath)
    //    val dataFrame = csvUtil.readCsv(sc, sqlContext, basePath + inputPath)
    viewDf(dataFrame, "dataFrame")
    //////////////////参数处理
    val modelSavePath = ConstantInfo.preModelPath + alPath
    //对lable进行index
    val dfLabelIndexer = new StringIndexer()
      .setInputCol(labelCol)
      .setOutputCol("indexedLabel").fit(dataFrame)
    val dfToUse = dfLabelIndexer.transform(dataFrame)


    val rddToUse = handleUtil.getLabelPointRdd(dfToUse, featuresCol, "indexedLabel")
    ////////////////////将算法作用于数据集
    val sVMWithSGD = new SVMWithSGD()
      .setIntercept(addIntercept.toBoolean)
      .setValidateData(validateData.toBoolean)
    val fitModel: SVMModel = sVMWithSGD.run(rddToUse)

    //    val testRdd = handleUtil.getVectorRdd(dataFrame, featuresCol)
    //    val resultRdd = fitModel.predict(testRdd).map(row => Row(row))
    //    testRdd.foreach(row => println(row))
    //    val resultDf = sqlContext.createDataFrame(resultRdd, handleUtil.getSchema("prediction double"))
    //    val finalDf = handleUtil.getFinalDf(dataFrame, resultDf, sqlContext)
    //    finalDf.show()
    //页面展现json

    implicit lazy val formats = Serialization.formats(NoTypeHints)
    val json = "[" +
      write(FinalVO(write(handleUtil.sortColNames(featuresCol)), fitModel.getThreshold.get,
        fitModel.intercept, write(fitModel.weights.toArray)
      )) + "]"
    println(json)
    ////////////////////保存结果及保存json
    fitModel.save(sc, basePath + modelSavePath)
    super.flagSparked(taskId.toInt, outputPath, "no head", json)
    dfLabelIndexer.write.overwrite().save(basePath + modelSavePath + "/labelIndexer")
  }

  case class FinalVO(cols: String, threshHold: Double, intercept: Double, weight: String)

}

object svm {

  def main(args: Array[String]): Unit = {
    val basePath = ""
    val inputPath = "data/tree.csv"

    val addIntercept = "false"
    val featuresCol = "col1,col2"
    val labelCol = "label"
    val modelSavePath = "svm/0815"
    val validateData = "true"

    val outputPath = ""
    val taskId = "182"

    //    execSVMWithSGD(basePath, inputPath,
    //      addIntercept, featuresCol, labelCol, modelSavePath,validateData,
    //      outputPath, taskId: String)
    exec(handleUtil.getContext("svm"), args)

  }

  def exec(sc: SparkContext, args: Array[String]): Unit = {
    implicit lazy val formats = org.json4s.jackson.Serialization.formats(org.json4s.NoTypeHints)
    case class ArgVO(inputPath: String,
                     addIntercept: String, featuresCol: String, labelCol: String,predictionCol: String, alPath: String,
                     validateData: String, outputPath: String)

    val executer = new svm(args.last)
    try {
      args.foreach { x => executer.logger.info("inArg-" + x) }
      val basePath = args(0)
      val ArgVO(inputPath: String,
                     addIntercept: String, featuresCol: String, labelCol: String,predictionCol: String, alPath: String,
                     validateData: String, outputPath: String)
      = org.json4s.jackson.Serialization.read[ArgVO](args(1))
      val taskId = args(2)
      //executer.checkArgsNum(9, args.length)
      executer.execSVMWithSGD(sc: SparkContext, basePath: String, inputPath: String,
                     addIntercept: String, featuresCol: String, labelCol: String,predictionCol: String, alPath: String,
                     validateData: String, outputPath: String, taskId: String)
    } catch {
      case ex: Throwable => executer.handleException(args(args.length - 1).toInt, ex, "svm")
    }
  }

}
