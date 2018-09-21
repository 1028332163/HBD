package com.esoft.dae.prediction.predictor

import com.esoft.dae.dao.BaseDao
import com.esoft.dae.util.handleUtil
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.StringIndexerModel
import org.apache.spark.mllib.classification.SVMModel
import org.apache.spark.sql.{Row, SQLContext, SaveMode}

/**
  * Created by asus on 2016/12/28.
  */
class svmPredictor(taskId: String) extends BaseDao(taskId) {
  def execPrediction(sc: SparkContext, basePath: String,
                     addCols: String, alPath: String, colNamesPara: String,
                     dfPath: String, predictionCol: String,
                      outputPath: String, taskId: String) {
    ///////////构建yarn-spark,从parquet中读取文件
    val sqlContext = new SQLContext(sc)
    val dataFrame = sqlContext.read.parquet(basePath + dfPath)
    viewDf(dataFrame, "dataFrame")
    //////////构建local-spark,从csv中读取文件
    //    val conf = new SparkConf().setAppName("test").setMaster("local")
    //    val sc = new SparkContext(conf)
    //    val sqlContext = new SQLContext(sc)
    //    val dataFrame = csvUtil.readCsv(sc, sqlContext, basePath + dfPath)
    //    viewDf(dataFrame,"dataFrame")
    //////////////////参数处理
    val dataRdd = handleUtil.getVectorRdd(dataFrame, colNamesPara)
    ////////////////////将算法作用于数据集
    val fitModel = SVMModel.load(sc, basePath + alPath)
    val labels = StringIndexerModel.load(basePath + alPath + "/labelIndexer").labels
    val predictRdd = fitModel.predict(dataRdd).map(result => Row(labels(result.toInt)))
    val predictDf = sqlContext.createDataFrame(predictRdd, handleUtil.getSchema(predictionCol + " string"))
    val finalDf = handleUtil.getFinalDf(dataFrame, predictDf)
    finalDf.show()
    ////////////////////页面展现用的df
    val showDf = handleUtil.indexDataFrame(finalDf.select(predictionCol, addCols.split(","): _*))
    showDf.show()
    val showDfPath = handleUtil.getDfShowPath(outputPath) + "1.parquet"
    showDf.write.format("parquet")
      .mode(SaveMode.Overwrite)
      .save(basePath + showDfPath)
    updateTaskInsData(taskId.toInt, showDfPath, showDf.dtypes, ONLY_ONE_TABLE, "no json")
    ////////////////////保存结果及保存json
    finalDf.write.format("parquet")
      .mode(SaveMode.Overwrite)
      .save(basePath + outputPath)
    super.flagSparked(taskId.toInt, outputPath, handleUtil.getHeadContent(finalDf.dtypes), "no json")
  }
}
