package com.esoft.dae.model

import com.esoft.dae.dao.BaseDao
import org.apache.spark.ml.feature.{StringIndexer, VectorIndexer}
import org.apache.spark.{SparkConf, SparkContext}
import com.esoft.dae.util.{ConstantInfo, handleUtil, csvUtil}
import org.apache.spark.sql.{SaveMode, SQLContext}
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import org.apache.spark.ml.classification.{GBTClassificationModel, GBTClassifier}
import org.json4s.jackson.Serialization.write

/**
  * @author
  */
class gbdtCls(taskId: String) extends BaseDao(taskId) {
  def execGBTClassifier(sc: SparkContext, basePath: String, inputPath: String,
                        checkpointInterval: String, featuresCol: String, impurity: String, labelCol: String,
                        lossType: String, maxBins: String, maxDepth: String, maxIter: String, minInfoGain: String,
                        minInstancesPerNode: String, alPath: String, predictionCol: String, seed: String,
                        stepSize: String, subsamplingRate: String,
                        outputPath: String, taskId: String): Unit = {
    ///////////构建yarn-spark,从parquet中读取文件
    val sqlContext = new SQLContext(sc)
    val dataFrame = sqlContext.read.parquet(basePath + inputPath)
    //    val dataFrame = csvUtil.readStrCsv(sc, sqlContext, basePath + inputPath)
    viewDf(dataFrame, "dataFrame")
    //////////////////参数处理
    val modelSavePath = ConstantInfo.preModelPath + alPath
    val combinedDf = handleUtil.combineCol(handleUtil.indexFeatures(dataFrame, featuresCol, basePath + modelSavePath), handleUtil.getClassfierFeatures(featuresCol, dataFrame.dtypes))
    combinedDf.show()
    //对特征列进行indexer
    val dfVectorIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .fit(combinedDf)
    val feaIndexedDf = dfVectorIndexer.transform(combinedDf)
    feaIndexedDf.show()
    dfVectorIndexer.write.overwrite().save(basePath + modelSavePath + "/vectorIndexer")
    //对lable进行index
    val dfLabelIndexer = new StringIndexer()
      .setInputCol(labelCol)
      .setOutputCol("indexedLabel").fit(feaIndexedDf)
    val dfToUse = dfLabelIndexer.transform(feaIndexedDf)
    dfLabelIndexer.write.overwrite().save(basePath + modelSavePath + "/labelIndexer")
    ////////////////////将算法作用于数据集
    val gBTClassifier = new GBTClassifier()
      .setFeaturesCol("indexedFeatures")
      .setImpurity(impurity)
      .setLabelCol("indexedLabel")
      .setLossType(lossType)
      .setMaxBins(maxBins.toInt)

      .setMaxDepth(maxDepth.toInt)
      .setMaxIter(maxIter.toInt)
      .setMinInfoGain(minInfoGain.toDouble)
      .setMinInstancesPerNode(minInstancesPerNode.toInt)
      .setPredictionCol(predictionCol)

      .setStepSize(stepSize.toDouble)
      .setSubsamplingRate(subsamplingRate.toDouble)
    //.setCheckpointInterval(checkpointInterval)
    //    .setSeed(seed)
    val model: GBTClassificationModel = gBTClassifier.fit(dfToUse)
    val finalDf = model.transform(dfToUse)
    finalDf.show()
    ////////////////////保存结果及保存json
    implicit lazy val formats = Serialization.formats(NoTypeHints)
    val trees = model.treeWeights.zip(model.trees).map(one => new Tree(one._1, one._2.numNodes, one._2.depth))
    val summaryVO = new SummaryVO(model.numFeatures, model.numTrees, model.totalNumNodes, write(trees))
    val json = "[" + write(summaryVO) + "]"
    sc.parallelize(Seq(model), 1).saveAsObjectFile(basePath + modelSavePath + "/model")
    super.flagSparked(taskId.toInt, outputPath, "no head", json)
  }

  case class Tree(weight: Double, numNodes: Int, depth: Int)

  case class SummaryVO(numFeatues: Int, numTrees: Int, totalNumNodes: Int, trees: String)

}

object gbdtCls {

  def main(args: Array[String]): Unit = {
    val basePath = ""
    val inputPath = "data/treeRgs.csv"

    val checkpointInterval = "10"
    val featuresCol = "col1,col2"
    val impurity = "gini" //entropy||gini
    val labelCol = "label" //
    val lossType = "logistic" //logistic
    val maxBins = "32"
    val maxDepth = "5"
    val maxIter = "20"
    val minInfoGain = "0.0"
    val minInstancesPerNode = "1"
    val modelSavePath = "cGbdt/0815"
    val predictionCol = "prediction"
    val seed = ""
    val stepSize = "0.1"
    val subsamplingRate = "1.0"

    val outputPath = ""
    val taskId = "182"

    //    val args = Array(basePath, inputPath,
    //      checkpointInterval, featuresCol, impurity, labelCol, lossType,
    //      maxBins, maxDepth, maxIter, minInfoGain, minInstancesPerNode, modelSavePath,
    //      predictionCol, seed, stepSize, subsamplingRate,
    //      outputPath, taskId: String)
    exec(handleUtil.getContext("gbdtCls"), args)

  }

  def exec(sc: SparkContext, args: Array[String]): Unit = {
    implicit lazy val formats = org.json4s.jackson.Serialization.formats(org.json4s.NoTypeHints)
    case class ArgVO(inputPath: String,
                        checkpointInterval: String, featuresCol: String, impurity: String, labelCol: String,
                        lossType: String, maxBins: String, maxDepth: String, maxIter: String, minInfoGain: String,
                        minInstancesPerNode: String, alPath: String, predictionCol: String, seed: String,
                        stepSize: String, subsamplingRate: String,
                        outputPath: String)

    val executer = new gbdtCls(args.last)
    try {
      args.foreach { x => executer.logger.info("inArg-" + x) }
      val basePath = args(0)
      val ArgVO(inputPath: String,
                        checkpointInterval: String, featuresCol: String, impurity: String, labelCol: String,
                        lossType: String, maxBins: String, maxDepth: String, maxIter: String, minInfoGain: String,
                        minInstancesPerNode: String, alPath: String, predictionCol: String, seed: String,
                        stepSize: String, subsamplingRate: String,
                        outputPath: String)
      = org.json4s.jackson.Serialization.read[ArgVO](args(1))
      val taskId = args(2)
      //executer.checkArgsNum(19, args.length)
      executer.execGBTClassifier(sc: SparkContext, basePath: String, inputPath: String,
                        checkpointInterval: String, featuresCol: String, impurity: String, labelCol: String,
                        lossType: String, maxBins: String, maxDepth: String, maxIter: String, minInfoGain: String,
                        minInstancesPerNode: String, alPath: String, predictionCol: String, seed: String,
                        stepSize: String, subsamplingRate: String,
                        outputPath: String, taskId: String)



    } catch {
      case ex: Throwable => executer.handleException(args(args.length - 1).toInt, ex, "gbdtCls")
    }
  }

}
