package com.esoft.dae.prediction

import com.esoft.dae.dao.BaseDao
import com.esoft.dae.prediction.predictor._
import com.esoft.dae.util.{ConstantInfo, handleUtil}
import org.apache.spark.SparkContext

/**
  * @author liuzw
  */
class prediction(taskId: String) extends BaseDao(taskId) {

  def execPrediction(sc: SparkContext, args: Array[String]): Unit = {
    args.foreach(println(_))
    handleUtil.getAl(args(2)) match {
      case "Regressionlinear" =>
        println("Regressionlinear")
        new LineRPredictor(sc, args(0), args(1),
          ConstantInfo.preModelPath + args(2), args(3), args(4), args(5), args(6),
          args(7)).run()
      case "LogisticRegression" =>
        println("logisticRegression")
        new LogisticRPredictor(sc, args(0), args(1),
          ConstantInfo.preModelPath + args(2), args(3), args(4), args(5), args(6),
          args(7)).run()
      case "RegressionOrdinal" =>
        new BasePredictor(sc, args(0), args(1),
          ConstantInfo.preModelPath + args(2), args(3), args(4), args(5), args(6),
          args(7)).run()
      case "ClusteringKMeans" =>
        println("kmeans")
        new KmeansPredictor(sc, args(0), args(1),
          ConstantInfo.preModelPath + args(2), args(3), args(4), args(5), args(6),
          args(7)).run()
      case "NaiveBayes" =>
        println("naiveBayes")
        new NaiveBayesPredictor(sc, args(0), args(1),
          ConstantInfo.preModelPath + args(2), args(3), args(4), args(5), args(6),
          args(7)).run()
      case "DecisionTreeClassification" =>
        println("cDecisionTree")
        new DTreeClsPredictor(sc, args(0), args(1),
          ConstantInfo.preModelPath + args(2), args(3), args(4), args(5), args(6),
          args(7)).run()
      case "DecisionTreeRegression" =>
        println("rDecisionTree")
        new DTreeRgsPredictor(sc, args(0), args(1),
          ConstantInfo.preModelPath + args(2), args(3), args(4), args(5), args(6),
          args(7)).run()
      case "RandomForestsClassification" =>
        println("cRandomForest")
        new RForestClsPredictor(sc, args(0), args(1),
          ConstantInfo.preModelPath + args(2), args(3), args(4), args(5), args(6),
          args(7)).run()
      case "RandomForestsRegression" =>
        println("rRandomForest")
        new RForestRgsPredictor(sc, args(0), args(1),
          ConstantInfo.preModelPath + args(2), args(3), args(4), args(5), args(6),
          args(7)).run()
      case "GBDTClassification" =>
        println("gbdtCls")
        new GbdtClsPredictor(sc, args(0), args(1),
          ConstantInfo.preModelPath + args(2), args(3), args(4), args(5), args(6),
          args(7)).run()
      case "GBDTRegression" =>
        println("gbdtRgs")
        new GbdtRgsPredictor(sc, args(0), args(1),
          ConstantInfo.preModelPath + args(2), args(3), args(4), args(5), args(6),
          args(7)).run()
      case "ClusteringHybridGauss" =>
        println("gmm")
        new gmmPredictor(args(7)).execPrediction(sc, args(0), args(1),
          ConstantInfo.preModelPath + args(2), args(3), args(4), args(5), args(6),
          args(7))
      case "RegressionVectorMachine" =>
        println("svm")
        new svmPredictor(args(7)).execPrediction(sc, args(0), args(1),
          ConstantInfo.preModelPath + args(2), args(3), args(4), args(5), args(6),
          args(7))
    }
  }

  override def getSpeHint(ex: Throwable): String = {
    val exString = ex.toString
    exString match {
      case notTrainFea if exString.contains("java.util.NoSuchElementException: key not found")
      => "测试集的特征值在训练集中未出现:"
      case _ => ""
    }
  }


}

object prediction {
  def main(args: Array[String]) {
    val basePath = ""
    val inputPath = ""

    val addCols = "label" //用户查看数据的时候要
    val alPath = "svm/0815" //模型的地址
    val colNamesPara = "col1,col2"
    val dfPath = "data/tree.csv" //数据集的地址
    val predictionCol = "prediction"

    val headerContent = "col1 double,col2 double"
    val outputPath = "logisticRegression/fitResultDf"
    val taskId = "1"

    //    val args = Array(basePath: String, inputPath: String,
    //      addCols: String, alPath: String, colNamesPara: String, dfPath: String, predictionCol: String,
    //       outputPath: String, taskId: String)
    exec(handleUtil.getContext("prediction"), args)
  }

  def exec(sc: SparkContext, args: Array[String]): Unit = {
    implicit lazy val formats = org.json4s.jackson.Serialization.formats(org.json4s.NoTypeHints)
    case class ArgVO(inputPath: String,
                     featuresColShow: String, featuresCol: String,
                     rightInputPath: String, predictionCol: String,
                     outputPath: String)

    val executer = new prediction(args.last)
    try {
      args.foreach { x => executer.logger.info("inArg-" + x) }
      val basePath = args(0)
      val ArgVO(inputPath: String,
      addCols: String, colNamesPara: String,
      rightInputPath: String, predictionCol: String,
      outputPath: String)
      = org.json4s.jackson.Serialization.read[ArgVO](args(1))
      val taskId = args(2)
      val exeArgs = Array(basePath,
        addCols: String, inputPath: String, colNamesPara: String,
        rightInputPath: String, predictionCol: String,
        outputPath: String, taskId)
      executer.execPrediction(sc, exeArgs)
    } catch {
      case ex: Throwable => executer.handleException(args(args.length - 1).toInt, ex, "prediction")
    }
  }

}