package com.esoft.dae.model

import com.esoft.dae.dao.BaseDao
import com.esoft.dae.util.{ConstantInfo, csvUtil, handleUtil}
import org.apache.spark.ml.feature.{StringIndexer, VectorIndexer}
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.{Row, SaveMode, SQLContext}
import org.apache.spark.ml.classification.{NaiveBayesModel, NaiveBayes}

/**
  * @author liuzw
  */
class naiveBayesClassifier(taskId: String) extends BaseDao(taskId) {
  def execNaiveBayes(sc: SparkContext, basePath: String, inputPath: String,
                     featuresCol: String, labelCol: String, alPath: String, modelType: String, predictionCol: String, probabilityCol: String, rawPredictionCol: String, smoothing: String, thresholds: String,
                     outputPath: String, taskId: String): Unit = {
    /////////构建yarn-spark,从parquet中读取文件
    val sqlContext = new SQLContext(sc)
    val dataFrame = sqlContext.read.parquet(basePath + inputPath)
    //              val dataFrame = csvUtil.readStrCsv(sc, sqlContext, basePath + inputPath)
    viewDf(dataFrame, "dataFrame")
    //////////////////参数处理
    val modelSavePath = ConstantInfo.preModelPath + alPath
    //根据用户的features，先对string的col进行stringIndexer，再将indexcer的结果合成一个向量
    val combinedDf = handleUtil.combineCol(handleUtil.indexFeatures(dataFrame, featuresCol, basePath + modelSavePath), handleUtil.getClassfierFeatures(featuresCol, dataFrame.dtypes))
    combinedDf.show()
    //对特征列进行indexer
    val dfVectorIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .fit(combinedDf)
    val feaIndexedDf = dfVectorIndexer.transform(combinedDf)
    feaIndexedDf.show()

    //对标签列进行index
    val dfLabelIndexer = new StringIndexer()
      .setInputCol(labelCol)
      .setOutputCol("indexedLabel").fit(feaIndexedDf)
    val dfToUse = dfLabelIndexer.transform(feaIndexedDf)
    dfToUse.show()
    //      if()
    dfLabelIndexer.write.overwrite().save(basePath + modelSavePath + "/labelIndexer")
    ////////////////////将算法作用于数据集
    val naiveBayes = new NaiveBayes()
      .setFeaturesCol("indexedFeatures")
      .setLabelCol("indexedLabel")
      .setModelType(modelType)
      .setPredictionCol(predictionCol)
      .setProbabilityCol(probabilityCol)
      .setRawPredictionCol(rawPredictionCol)
      .setSmoothing(smoothing.toDouble)
    if (thresholds != "") {
      //      naiveBayes.setThresholds(thresholds)
    }

    val fitModel: NaiveBayesModel = naiveBayes.fit(dfToUse)
    //去掉了rawPredictionCol,并将probability向量的第二个元素取出，hive建不了有vector的df
    val finalCols = dataFrame.columns :+ predictionCol :+ probabilityCol
    val fittedDf = fitModel.transform(dfToUse)
    fittedDf.show(false)
    //页面展现的df
    logger.info("numClasses:" + fitModel.numClasses +
      ",numFeatures:" + fitModel.numFeatures +
      ",pi:" + fitModel.pi)
    //    概率矩阵
    val theta = fitModel.theta
    println(theta.numRows, theta.numCols)
    val test = theta.toArray
    test.foreach(println(_))
    ////////////////////保存结果及保存json
    dfVectorIndexer.write.overwrite().save(basePath + modelSavePath + "/vectorIndexer")
    fitModel.write.overwrite().save(basePath + modelSavePath + "/model")
    super.flagSparked(taskId.toInt, outputPath, "no head", "no json")
  }

  override def getSpeHint(ex: Throwable): String = {
    val exString = ex.toString
    exString match {
      case feaNumErr if exString.contains(" Bernoulli naive Bayes requires 0 or 1 feature values")
      => "特征列取值的个数大于2，不能使用Bernoulli概率模型:"
      case _ => ""
    }
  }

}

object naiveBayesClassifier {

  def main(args: Array[String]): Unit = {
    val basePath = ""
    val inputPath = "data/tree.csv"

    val featuresCol = "col1,col2" //String 特征列
    val labelCol = "label" // String 标签列
    val modelSavePath = "naiveBayes/0815" //模型保存路径
    val modelType = "multinomial" //String 概率模型类型 multinomial||bernoulli
    val predictionCol = "prediction" //String 结果列
    val probabilityCol = "probability" //Stirng 概率列
    val rawPredictionCol = "rawPrediction" //String 原概率列
    val smoothing = "1.0" //double 平滑系数 default:1.0
    val thresholds = "" //暂时还不明白，先传个null过来

    val outputPath = ""
    val taskId = "182"


    //        val args = Array(basePath, inputPath,
    //          featuresCol, labelCol, modelSavePath, modelType, predictionCol, probabilityCol, rawPredictionCol, smoothing, thresholds,
    //          outputPath, taskId: String)
    exec(handleUtil.getContext("naiveBayesClassifier"), args)
  }

  def exec(sc: SparkContext, args: Array[String]): Unit = {
    implicit lazy val formats = org.json4s.jackson.Serialization.formats(org.json4s.NoTypeHints)
    case class ArgVO(inputPath: String,
                     featuresCol: String, labelCol: String, alPath: String, modelType: String, predictionCol: String, probabilityCol: String, rawPredictionCol: String, smoothing: String, thresholds: String,
                     outputPath: String)

    val executer = new naiveBayesClassifier(args.last)
    try {
      args.foreach { x => executer.logger.info("inArg-" + x) }
      val basePath = args(0)
      val ArgVO(inputPath: String,
                     featuresCol: String, labelCol: String, alPath: String, modelType: String, predictionCol: String, probabilityCol: String, rawPredictionCol: String, smoothing: String, thresholds: String,
                     outputPath: String)
      = org.json4s.jackson.Serialization.read[ArgVO](args(1))
      val taskId = args(2)
      //executer.checkArgsNum(13, args.length)
      executer.execNaiveBayes(sc: SparkContext, basePath: String, inputPath: String,
                     featuresCol: String, labelCol: String, alPath: String, modelType: String, predictionCol: String, probabilityCol: String, rawPredictionCol: String, smoothing: String, thresholds: String,
                     outputPath: String, taskId: String)


    } catch {
      case ex: Throwable => executer.handleException(args(args.length - 1).toInt, ex, "naiveBayesClassifier")
    }
  }

}
