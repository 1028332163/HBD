package com.esoft.dae.model

import com.esoft.dae.dao.BaseDao
import org.apache.spark.{SparkConf, SparkContext}
import com.esoft.dae.util.{ConstantInfo, handleUtil, csvUtil}
import org.apache.spark.mllib.linalg.{VectorUDT, Vectors}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.{Row, SaveMode, SQLContext}
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization._
import org.json4s.jackson.Serialization
import org.apache.spark.mllib.clustering.GaussianMixture
import org.apache.spark.mllib.linalg.Matrix

/**
  * @author
  */
class gmm(taskId: String) extends BaseDao(taskId) {
  def execGaussianMixture(sc: SparkContext, basePath: String, inputPath: String,
                          convergence: String, featuresCol: String, k: String,
                          maxIterations: String, alPath: String, predictionCol: String, seed: String,
                          outputPath: String, taskId: String): Unit = {
    ///////////构建yarn-spark,从parquet中读取文件
    val sqlContext = new SQLContext(sc)
    val dataFrame = sqlContext.read.parquet(basePath + inputPath)
    //            val dataFrame = csvUtil.readCsv(sc, sqlContext, basePath + inputPath)
    viewDf(dataFrame, "dataFrame")
    //////////////////参数处理
    val modelSavePath = ConstantInfo.preModelPath + alPath
    val dataRdd = handleUtil.getVectorRdd(dataFrame, featuresCol)
    ////////////////////将算法作用于数据集
    val gaussianMixture = new GaussianMixture()
      .setConvergenceTol(convergence.toDouble)
      .setK(k.toInt)
      .setMaxIterations(maxIterations.toInt)
    if(""!=seed)
      gaussianMixture.setSeed(seed.asInstanceOf[Long])
    val fitModel = gaussianMixture.run(dataRdd)

    val predictRdd = fitModel.predict(dataRdd).map(result => Row(result))
    val predictDf = sqlContext.createDataFrame(predictRdd, handleUtil.getSchema(predictionCol + " int"))
    val finalDf = handleUtil.getFinalDf(dataFrame, predictDf)
    finalDf.write.format("parquet")
      .mode(SaveMode.Overwrite)
      .save(basePath + outputPath)

    val models = fitModel.weights.zip(fitModel.gaussians).map(tuple => G_Model(tuple._1, tuple._2.mu, tuple._2.sigma))
    val finalVo = FinalVO(fitModel.k, handleUtil.sortColNames(featuresCol), models)
    implicit lazy val formats = Serialization.formats(NoTypeHints)
    val json = "[" + write(ShowVO("gmm_model", write(finalVo))) + "]"
    println(json)
    ////////////////////保存结果及保存json
    fitModel.save(sc, basePath + modelSavePath)
    super.flagSparked(taskId.toInt, outputPath, handleUtil.getHeadContent(finalDf.dtypes), json)
  }

  case class G_Model(weight: Double, mu: Vector, sigma: Matrix)

  case class FinalVO(k: Int, cols: Array[String], models: Array[G_Model])

  case class ShowVO(summary: String, json: String)

}

object gmm {

  def main(args: Array[String]): Unit = {
    val basePath = ""
    val inputPath = "data/tree.csv"

    val convergence = "0.01" //收敛标准
    val featuresCol = "col1,col2" //特征列
    val clusterNum = "2" //聚类个数
    val maxIterations = "100" //最大迭代次数
    val modelSavePath = "gmm/0815" //模型保存路径
    val predictionCol = "prediction"
    val seed = ""

    val outputPath = ""
    val taskId = "182"

    //    val args = Array(basePath, inputPath,
    //      convergence, featuresCol, k, maxIterations, modelSavePath, predictionCol, seed,
    //      outputPath, taskId: String)
    exec(handleUtil.getContext("gmm"), args)

  }

  def exec(sc: SparkContext, args: Array[String]): Unit = {
    implicit lazy val formats = org.json4s.jackson.Serialization.formats(org.json4s.NoTypeHints)
    case class ArgVO(inputPath: String,
                          convergence: String, featuresCol: String, clusterNum: String,
                          maxIterations: String, alPath: String, predictionCol: String, seed: String,
                          outputPath: String)

    val executer = new gmm(args.last)
    try {
      args.foreach { x => executer.logger.info("inArg-" + x) }
      val basePath = args(0)
      val ArgVO(inputPath: String,
                          convergence: String, featuresCol: String, k: String,
                          maxIterations: String, alPath: String, predictionCol: String, seed: String,
                          outputPath: String)
      = org.json4s.jackson.Serialization.read[ArgVO](args(1))
      val taskId = args(2)
      //executer.checkArgsNum(11, args.length)
      executer.execGaussianMixture(sc: SparkContext, basePath: String, inputPath: String,
                          convergence: String, featuresCol: String, k: String,
                          maxIterations: String, alPath: String, predictionCol: String, seed: String,
                          outputPath: String, taskId: String)


    } catch {
      case ex: Throwable => executer.handleException(args(args.length - 1).toInt, ex, "gmm")
    }
  }

}
