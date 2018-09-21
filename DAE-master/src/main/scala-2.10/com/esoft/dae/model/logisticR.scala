package com.esoft.dae.model

import com.esoft.dae.dao.BaseDao
import com.esoft.dae.util.{ConstantInfo, csvUtil, handleUtil}
import org.apache.spark.ml.classification.{LogisticRegressionModel, LogisticRegression}
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.{SaveMode, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author liuzw
  */
class logisticR(taskId: String) extends BaseDao(taskId) {
  def LogisticR(sc: SparkContext, basePath: String, inputPath: String,
                elasticNetParam: String, featuresCol: String, fitIntercept: String, labelCol: String, maxIter: String,
                alPath: String, predictionCol: String, probabilityCol: String, rawPredictionCol: String, regParam: String,
                standardization: String, threshold: String, thresholds: String, tol: String, weightCol: String,
                outputPath: String, taskId: String) {
    ///////构建yarn-spark,从parquet中读取文件
    val sqlContext = new SQLContext(sc)
    val dataFrame = sqlContext.read.parquet(basePath + inputPath)
    //        val dataFrame = csvUtil.readCsv(sc, sqlContext, basePath + inputPath)
    viewDf(dataFrame, "dataFrame")
    ///////////参数处理代码
    val modelSavePath = ConstantInfo.preModelPath + alPath
    val combinedCol = handleUtil.combineCol(dataFrame, featuresCol) //可以将输入的多个特征列放到fetures向量中
    //对lable进行index
    val dfLabelIndexer = new StringIndexer()
        .setInputCol(labelCol)
        .setOutputCol("indexedLabel").fit(combinedCol)
    val dfToUse = dfLabelIndexer.transform(combinedCol)

    ///////////算法处理代码
    val lr = new LogisticRegression()
      .setFeaturesCol("features")
      .setLabelCol("indexedLabel")
      .setElasticNetParam(elasticNetParam.toDouble)
      .setFitIntercept(fitIntercept.toBoolean)
      .setMaxIter(maxIter.toInt)
      .setProbabilityCol(probabilityCol)
      .setRawPredictionCol(rawPredictionCol)
      .setRegParam(regParam.toDouble)
      .setStandardization(standardization.toBoolean)
      .setThreshold(threshold.toDouble)
      .setTol(tol.toDouble)
    if ("" != weightCol) {
      lr.setWeightCol(weightCol)
    }
    /////////////模型fit
    val fitModel: LogisticRegressionModel = lr.fit(dfToUse)
    ////////////模型总结
    //每一列的系数
    val colNames = handleUtil.sortColNames(featuresCol)
    val summary = fitModel.summary
    val intercept = fitModel.intercept
    val values = fitModel.coefficients
    val infoArr =
      ((for (i <- colNames.indices) yield Array(colNames(i), values(i)))
        :+ Array("intercept", intercept))
        .map(row => Row.fromSeq(row))

    val showHead = handleUtil.getSchema("colName string,coefficient double")
    val showDf = sqlContext.createDataFrame(sc.parallelize(infoArr), showHead)
    println(ONLY_ONE_TABLE + "***********")
    showDf.show()
    //////////数据保存
    val showDfPath = handleUtil.getDfShowPath(outputPath) + "1.parquet"
    showDf.write.format("parquet")
      .mode(SaveMode.Overwrite)
      .save(basePath + showDfPath)
    updateTaskInsData(taskId.toInt, showDfPath, showDf.dtypes, ONLY_ONE_TABLE, "no json")
    fitModel.write.overwrite().save(basePath + modelSavePath)
    dfLabelIndexer.write.overwrite().save(basePath + modelSavePath + "/labelIndexer")
    super.flagSparked(taskId.toInt, outputPath, "no head", "no json")
  }

}

object logisticR {

  def main(args: Array[String]): Unit = {

    val basePath = ""
    val inputPath = "data/tree.csv"

    val elasticNetParam = "0" //[0,1] 0->L2,1->L1,两个正则化类型的混合系数;
    val featuresCol = "col1,col2" //特征列
    val fitIntercept = "true" //是否拟合截距
    val labelCol = "label" //标签列
    val maxIter = "100" //最大迭代次数
    val modelSavePath = "logisticRegression/logisticRModel"
    val predictionCol = "prediction" //结果列
    val probabilityCol = "probability" //
    val rawPredictionCol = "rawPrediction"
    val regParam = "0" //正则系数：>=0的double数值
    val standardization = "true" //处理之前是否进行标准化
    val threshold = "0.5" //[0,1]用来分类的阈值
    val thresholds = "" //Array[Double]
    val tol = "1E-6" //判断收敛的阈值
    val weightCol = "" //标识记录权重的列

    val outputPath = ""
    val taskId = "815"
    //        val args = Array(sc: SparkContext, basePath: String, inputPath: String,
    //          elasticNetParam: String, featuresCol: String, fitIntercept: String, labelCol: String, maxIter: String,
    //          modelSavePath: String, predictionCol: String, probabilityCol: String, rawPredictionCol: String, regParam: String,
    //          standardization: String, threshold: String, thresholds: String, tol: String, weightCol: String,
    //          outputPath: String, taskId: String)

    exec(handleUtil.getContext("logisticR"), args)

  }

  def exec(sc: SparkContext, args: Array[String]): Unit = {
    implicit lazy val formats = org.json4s.jackson.Serialization.formats(org.json4s.NoTypeHints)
    case class ArgVO(inputPath: String,
                elasticNetParam: String, featuresCol: String, fitIntercept: String, labelCol: String, maxIter: String,
                alPath: String, predictionCol: String, probabilityCol: String, rawPredictionCol: String, regParam: String,
                standardization: String, threshold: String, thresholds: String, tol: String, weightCol: String,
                outputPath: String)

    val executer = new logisticR(args.last)
    try {
      args.foreach { x => executer.logger.info("inArg-" + x) }
      val basePath = args(0)
      val ArgVO(inputPath: String,
                elasticNetParam: String, featuresCol: String, fitIntercept: String, labelCol: String, maxIter: String,
                alPath: String, predictionCol: String, probabilityCol: String, rawPredictionCol: String, regParam: String,
                standardization: String, threshold: String, thresholds: String, tol: String, weightCol: String,
                outputPath: String)
      = org.json4s.jackson.Serialization.read[ArgVO](args(1))
      val taskId = args(2)
      //executer.checkArgsNum(19, args.length)
      executer.LogisticR(sc: SparkContext, basePath: String, inputPath: String,
                elasticNetParam: String, featuresCol: String, fitIntercept: String, labelCol: String, maxIter: String,
                alPath: String, predictionCol: String, probabilityCol: String, rawPredictionCol: String, regParam: String,
                standardization: String, threshold: String, thresholds: String, tol: String, weightCol: String,
                outputPath: String, taskId: String)




    } catch {
      case ex: Throwable => executer.handleException(args(args.length - 1).toInt, ex, "lineR")
    }
  }

}
