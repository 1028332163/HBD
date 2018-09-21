package com.esoft.dae.prediction.predictor

import com.esoft.dae.dao.BaseDao
import com.esoft.dae.util.handleUtil
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.GaussianMixtureModel
import org.apache.spark.sql.{Row, SQLContext, SaveMode}

/**
  * Created by asus on 2016/12/27.
  */
class gmmPredictor(taskId: String) extends BaseDao(taskId) {
  def execPrediction(sc: SparkContext, basePath: String,
                     addCols: String, alPath: String, colNamesPara: String,
                     dfPath: String, predictionCol: String,
                      outputPath: String, taskId: String) {
    ///////////构建yarn-spark,从parquet中读取文件
    val sqlContext = new SQLContext(sc)
    val dataFrame = sqlContext.read.parquet(basePath + dfPath)
    //    val dataFrame = csvUtil.readCsv(sc, sqlContext, basePath + dfPath)
    viewDf(dataFrame, "dataFrame")
    //////////////////参数处理
    val dataRdd = handleUtil.getVectorRdd(dataFrame, colNamesPara)
    ////////////////////将算法作用于数据集
    val fitModel = GaussianMixtureModel.load(sc, basePath + alPath)
    val predictRdd = fitModel.predict(dataRdd).map(result => Row(result))
    val predictDf = sqlContext.createDataFrame(predictRdd, handleUtil.getSchema(predictionCol + " int"))
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
