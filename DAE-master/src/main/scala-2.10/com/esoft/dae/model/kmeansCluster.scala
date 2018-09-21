package com.esoft.dae.model

import com.esoft.dae.dao.BaseDao
import com.esoft.dae.util.{ConstantInfo, csvUtil, handleUtil}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.sql.{Row, SaveMode, SQLContext}

/**
  * @author liuzw
  */
class kmeansCluster(taskId: String) extends BaseDao(taskId) {

  def execKmeans(sc: SparkContext, basePath: String, inputPath: String,
                 addCols: String, colNamesPara: String, clusterNum: String, maxItr: String, alPath: String,
                 predictionCol: String, seed: String, tol: String,
                 outputPath: String, taskId: String): Unit = {
    ///////构建yarn-spark,从parquet中读取文件
    val sqlContext = new SQLContext(sc)
    val dataFrame = sqlContext.read.parquet(basePath + inputPath)
    //        val dataFrame = csvUtil.readCsv(sc, sqlContext, basePath + inputPath)
    viewDf(dataFrame, "dataFrame")
    //////////////////参数处理
    val modelSavePath = ConstantInfo.preModelPath + alPath
    val dfToUse = handleUtil.combineCol(dataFrame, colNamesPara)
    ////////////////////将算法作用于数据集
    val kmeans = new KMeans()
      .setFeaturesCol("features")
      .setK(clusterNum.toInt)
      .setPredictionCol(predictionCol)
      .setMaxIter(maxItr.toInt)
      .setTol(tol.toDouble)
    if ("" != seed) {
      kmeans.setSeed(seed.toInt)
    }
    val fitModel = kmeans.fit(dfToUse)

    ////////////////////页面展现DF-1 col,col2,cluster
    val showDfCols = if (addCols != "")
      addCols.split(",") :+ predictionCol
    else
      Array(predictionCol)
    val showDf = handleUtil.indexDataFrame(fitModel.transform(dfToUse)
      .select(showDfCols.head, showDfCols.tail: _*))
    showDf.show()
    ////////////////////页面展现DF-2 cluster，centers，count
    val chartRows = new Array[Row](clusterNum.toInt)
    val chartSchema = handleUtil.getSchema("cluster int,count string" +
      handleUtil.sortColNames(colNamesPara).map(colName => "," + colName + " string").mkString("")
    )
    val centers = fitModel.clusterCenters

    val preDf = showDf.select(predictionCol)
    for (i <- centers.indices) {
      //计算chartDf中每一行的数据信息
      val cluster = i + 1
      val center = centers(i).toArray.map(_.toString)
      val count = sc.accumulator(0)
      preDf.foreach(row => if (row(0) == i) count += 1)
      val row = cluster +: count.value.toString +: center
      chartRows(i) = Row.fromSeq(row)
    }
    val chartDf = sqlContext.createDataFrame(sc.parallelize(chartRows), chartSchema)
    chartDf.show()
    ////////////////////保存模型及保存json
    //    保存页面df
    val showDfPath = handleUtil.getDfShowPath(outputPath) + "1.parquet"
    showDf.write.format("parquet")
      .mode(SaveMode.Overwrite)
      .save(basePath + showDfPath)
    updateTaskInsData(taskId.toInt, showDfPath, showDf.dtypes, ONLY_ONE_TABLE, "no json")
    val chartDfPath = handleUtil.getDfShowPath(outputPath) + "2.parquet"
    chartDf.write.format("parquet")
      .mode(SaveMode.Overwrite)
      .save(basePath + chartDfPath)
    updateTaskInsData(taskId.toInt, chartDfPath, chartDf.dtypes, MODEL_JSON, "no json")
    //保存模型和结果
    fitModel.write.overwrite().save(basePath + modelSavePath)
    super.flagSparked(taskId.toInt, outputPath, "no head", "no json")
  }

}

object kmeansCluster {

  def main(args: Array[String]): Unit = {
    val basePath = ""
    val inputPath = "data/tree.csv"

    val addCols = "col1,col2" //将原始DF的哪几列放到最后给用户看的json中去
    val colNamesPara = "col2,col1"
    val clusterNum = "2"
    val maxItr = "20"
    val modelSavePath = "outModel/kmeans/KmeansModel"
    val predictionCol = "prediction"
    val seed = ""
    val tol = "1e-4"

    val outputPath = ""
    val taskId = "1"
    //    val args = Array(basePath, inputPath,
    //      addCols, colNamesPara, k, maxItr, modelSavePath, predictionCol, seed, tol,
    //      outputPath, taskId: String)
    //    val sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local"))
    //    exec(sc, args)


    exec(handleUtil.getContext("kmeansCluster"), args)

  }

  def exec(sc: SparkContext, args: Array[String]): Unit = {
    implicit lazy val formats = org.json4s.jackson.Serialization.formats(org.json4s.NoTypeHints)
    case class ArgVO(inputPath: String,
                     featuresColShow: String, featuresCol: String, clusterNum: String,
                     maxItr: String, alPath: String,
                     predictionCol: String, seed: String, tol: String,
                     outputPath: String)

    val executer = new kmeansCluster(args.last)
    try {
      args.foreach(println(_))
      val basePath = args(0)
      val ArgVO(inputPath: String,
      addCols: String, colNamesPara: String, clusterNum: String, maxItr: String, alPath: String,
      predictionCol: String, seed: String, tol: String,
      outputPath: String)
      = org.json4s.jackson.Serialization.read[ArgVO](args(1))
      val taskId = args(2)
      //executer.checkArgsNum(12, args.length)
      executer.execKmeans(sc: SparkContext, basePath: String, inputPath: String,
        addCols: String, colNamesPara: String, clusterNum: String, maxItr: String, alPath: String,
        predictionCol: String, seed: String, tol: String,
        outputPath: String, taskId: String)


    } catch {
      case ex: Throwable => executer.handleException(args(args.length - 1).toInt, ex, "kmeansCluster")
    }
  }

}
