//package com.esoft.dae.model
//
//import com.esoft.dae.dao.BaseDao
//import com.esoft.dae.util.{ConstantInfo, csvUtil, handleUtil}
//import org.apache.spark.ml.feature.{StringIndexer, VectorIndexer}
//import org.apache.spark.ml.regression.{RandomForestRegressionModel, RandomForestRegressor}
//import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.sql.{DataFrame, SQLContext}
//import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
//import org.json4s.NoTypeHints
//import org.json4s.jackson.Serialization
//import org.json4s.jackson.Serialization._
//
//import scala.collection.immutable.IndexedSeq
//
///**
//  * @author liuzw
//  */
//class randomForest(taskId: String) extends BaseDao(taskId) {
//  def execRandomForestClassifier(sc: SparkContext, basePath: String, inputPath: String,
//                                 checkpointInterval: String, featureSubsetStrategy: String, featuresCol: String,
//                                 impurity: String, labelCol: String, maxBins: String, maxDepth: String, minInfoGain: String,
//                                 minInstancesPerNode: String, alPath: String, numTrees: String, predictionCol: String, probabilityCol: String,
//                                 rawPredictionCol: String, seed: String, subsamplingRate: String, thresholds: String,
//                                 outputPath: String, taskId: String): Unit = {
//    /////////构建yarn-spark,从parquet中读取文件
//    val sqlContext = new SQLContext(sc)
//    val dataFrame = sqlContext.read.parquet(basePath + inputPath)
//    //    val dataFrame = csvUtil.readStrCsv(sc, sqlContext, basePath + inputPath)
//    viewDf(dataFrame, "dataFrame")
//    //////////////////参数处理
//    val modelSavePath = ConstantInfo.preModelPath + alPath
//    //根据用户的features，先对string的col进行stringIndexer，再将indexcer的结果合成一个向量
//    val combinedDf = handleUtil.combineCol(handleUtil.indexFeatures(dataFrame, featuresCol, basePath + modelSavePath), handleUtil.getClassfierFeatures(featuresCol, dataFrame.dtypes))
//    combinedDf.show()
//    //对特征列进行indexer
//    val dfVectorIndexer = new VectorIndexer()
//      .setInputCol("features")
//      .setOutputCol("indexedFeatures")
//      .fit(combinedDf)
//    val feaIndexedDf = dfVectorIndexer.transform(combinedDf)
//    feaIndexedDf.show()
//    dfVectorIndexer.write.overwrite().save(basePath + modelSavePath + "/vectorIndexer")
//    var json = ""
//    ///////////////将算法作用于数据集
//    if ("cRandomForest".equals(handleUtil.getAl(alPath))) {
//      //对特征列进行index
//      val dfLabelIndexer = new StringIndexer()
//        .setInputCol(labelCol)
//        .setOutputCol("indexedLabel").fit(feaIndexedDf)
//      val dfToUse = dfLabelIndexer.transform(feaIndexedDf)
//      dfToUse.show()
//      dfLabelIndexer.write.overwrite().save(basePath + modelSavePath + "/labelIndexer")
//      val model = new RandomForestClassifier()
//        .setFeatureSubsetStrategy(featureSubsetStrategy)
//        .setFeaturesCol("indexedFeatures")
//        .setImpurity(impurity)
//        .setLabelCol("indexedLabel")
//        .setMaxBins(maxBins.toInt)
//
//        .setMaxDepth(maxDepth.toInt)
//        .setMinInfoGain(minInfoGain.toDouble)
//        .setMinInstancesPerNode(minInstancesPerNode.toInt)
//        .setNumTrees(numTrees.toInt)
//        .setPredictionCol(predictionCol)
//
//        .setProbabilityCol(probabilityCol)
//        .setRawPredictionCol(rawPredictionCol)
//        .setSubsamplingRate(subsamplingRate.toDouble)
//        //        .setSeed(seed.toLong)
//        //        .setThresholds()
//        //    .setCheckpointInterval(checkpointInterval)
//        .fit(dfToUse)
//      val finalDf: DataFrame = model.transform(dfToUse)
//      finalDf.show()
//      sc.parallelize(Seq(model), 1).saveAsObjectFile(basePath + modelSavePath + "/model")
//      json = getJson(model, handleUtil.sortColNames(featuresCol))
//    } else if ("rRandomForest".equals(handleUtil.getAl(alPath))) {
//      val dfToUse = handleUtil.fillLabelCol(feaIndexedDf, labelCol)
//      dfToUse.show()
//      //checkin,seed没有设置
//      val model = new RandomForestRegressor()
//        .setFeatureSubsetStrategy(featureSubsetStrategy)
//        .setFeaturesCol("indexedFeatures")
//        .setImpurity(impurity)
//        .setLabelCol(labelCol)
//        .setMaxBins(maxBins.toInt)
//
//        .setMaxDepth(maxDepth.toInt)
//        .setMinInfoGain(minInfoGain.toDouble)
//        .setMinInstancesPerNode(minInstancesPerNode.toInt)
//        .setNumTrees(numTrees.toInt)
//        .setPredictionCol(predictionCol)
//
//        .setSubsamplingRate(subsamplingRate.toDouble)
//        //        .setSeed(seed.toLong)
//        //    .setCheckpointInterval(checkpointInterval)
//        .fit(dfToUse)
//      val finalDf = model.transform(dfToUse)
//      finalDf.show()
//      sc.parallelize(Seq(model), 1).saveAsObjectFile(basePath + modelSavePath + "/model")
//      json = getJson(model, handleUtil.sortColNames(featuresCol))
//    }
//    ////////////////////保存结果及保存json
//    println(json)
//    super.flagSparked(taskId.toInt, outputPath, "no head", json)
//  }
//
//  //得到用于页面展现的json
//
//  def getJson(model: Any, features: Array[String]): String = {
//    implicit lazy val formats = Serialization.formats(NoTypeHints)
//
//
//    val summaryVO = model match {
//      case c: RandomForestClassificationModel =>
//        c.asInstanceOf[RandomForestClassificationModel]
//        val featureImportances = features.zip(c.featureImportances.toArray).map(one => new FeatureImportance(one._1, one._2))
//        val trees = c.treeWeights.zip(c.trees).map(one => new Tree(one._1, one._2.numNodes, one._2.depth))
//        new SummaryVO(c.numFeatures, c.numClasses, c.numTrees, c.totalNumNodes,
//          write(featureImportances), write(trees))
//      case r: RandomForestRegressionModel =>
//        r.asInstanceOf[RandomForestRegressionModel]
//        val featureImportances = features.zip(r.featureImportances.toArray).map(one => new FeatureImportance(one._1, one._2))
//        val trees = r.treeWeights.zip(r.trees).map(one => new Tree(one._1, one._2.numNodes, one._2.depth))
//        new SummaryVO(r.numFeatures, 0, r.numTrees, r.totalNumNodes,
//          write(featureImportances), write(trees))
//    }
//
//
//    "[" + write(summaryVO) + "]"
//  }
//
//  case class Tree(weight: Double, numNodes: Int, depth: Int)
//
//  case class FeatureImportance(feature: String, importance: Double)
//
//  case class SummaryVO(numFeatues: Int, numClasses: Int, numTrees: Int, totalNumNodes: Int,
//                       featureImportances: String, trees: String)
//
//}
//
//object randomForest {
//
//  def main(args: Array[String]): Unit = {
//    val basePath = ""
//    val inputPath = "data/tree.csv"
//
//    val checkpointInterval = "" //Int -1或者>=1 迭代几次进行一次磁盘写操作
//    val featureSubsetStrategy = "auto" //String auto||all||onethird||sqrt||log2
//    val featuresCol = "col2,col1" //important String e.g. "col1,col2"  那些列作为特征列
//    val impurity = "gini" //String entropy||gini 纯度的评价标准
//    val labelCol = "label" //important Stirng 标签列
//    val maxBins = "32" //Int 连续变量最多可以被分为几部分
//    val maxDepth = "5" //Int 树的最大深度
//    val minInfoGain = "0" //double 最小的纯度改善
//    val minInstancesPerNode = "1" //int 每个节点的最小实例数
//    val modelSavePath = "cRandomForest/RdForestModel" //String 模型的保存路径
//    val numTrees = "20" //int 树的数量
//    val predictionCol = "prediction" //important String 预测的结果输出列
//    val probabilityCol = "probability" //String 概率输出列
//    val rawPredictionCol = "rawPrediction" //String rawPrediction输出列
//    val seed = "" //long 随机种子
//    val subsamplingRate = "1.0" //double (0, 1] 用来训练的数据集占总数据集的比率
//    val thresholds = ""
//
//    val outputPath = ""
//    val taskId = "182"
//
//    //    val args = Array(sc: SparkContext, basePath: String, inputPath: String,
//    //      checkpointInterval: String, featureSubsetStrategy: String, featuresCol: String,
//    //      impurity: String, labelCol: String, maxBins: String, maxDepth: String, minInfoGain: String,
//    //      minInstancesPerNode: String, modelSavePath: String, numTrees: String, predictionCol: String, probabilityCol: String,
//    //      rawPredictionCol: String, seed: String, subsamplingRate: String, thresholds: String,
//    //      outputPath: String, taskId: String)
//    exec(handleUtil.getContext("randomForest"), args)
//
//  }
//
//  def exec(sc: SparkContext, args: Array[String]): Unit = {
//    implicit lazy val formats = org.json4s.jackson.Serialization.formats(org.json4s.NoTypeHints)
//    case class ArgVO(inputPath: String,
//                                 checkpointInterval: String, featureSubsetStrategy: String, featuresCol: String,
//                                 impurity: String, labelCol: String, maxBins: String, maxDepth: String, minInfoGain: String,
//                                 minInstancesPerNode: String, alPath: String, numTrees: String, predictionCol: String, probabilityCol: String,
//                                 rawPredictionCol: String, seed: String, subsamplingRate: String, thresholds: String,
//                                 outputPath: String)
//
//    val executer = new randomForest(args.last)
//    try {
//      args.foreach { x => executer.logger.info("inArg-" + x) }
//      val basePath = args(0)
//      val ArgVO(inputPath: String,
//                                 checkpointInterval: String, featureSubsetStrategy: String, featuresCol: String,
//                                 impurity: String, labelCol: String, maxBins: String, maxDepth: String, minInfoGain: String,
//                                 minInstancesPerNode: String, alPath: String, numTrees: String, predictionCol: String, probabilityCol: String,
//                                 rawPredictionCol: String, seed: String, subsamplingRate: String, thresholds: String,
//                                 outputPath: String)
//      = org.json4s.jackson.Serialization.read[ArgVO](args(1))
//      val taskId = args(2)
//      //executer.checkArgsNum(21, args.length)
//      executer.execRandomForestClassifier(sc: SparkContext, basePath: String, inputPath: String,
//                                 checkpointInterval: String, featureSubsetStrategy: String, featuresCol: String,
//                                 impurity: String, labelCol: String, maxBins: String, maxDepth: String, minInfoGain: String,
//                                 minInstancesPerNode: String, alPath: String, numTrees: String, predictionCol: String, probabilityCol: String,
//                                 rawPredictionCol: String, seed: String, subsamplingRate: String, thresholds: String,
//                                 outputPath: String, taskId: String)
//
//
//
//    } catch {
//      case ex: Throwable => executer.handleException(args(args.length - 1).toInt, ex, "randomForest")
//    }
//  }
//
//}
