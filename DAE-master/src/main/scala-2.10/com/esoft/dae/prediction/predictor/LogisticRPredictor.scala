package com.esoft.dae.prediction.predictor

import org.apache.spark.SparkContext

/**
  * Created by asus on 2016/12/14.
  */
class LogisticRPredictor(sc: SparkContext, basePath: String,
                         addCols: String, alPath: String, colNamesPara: String,
                         dfPath: String, predictionCol: String,
                          outputPath: String, taskId: String)
  extends BasePredictor(sc: SparkContext, basePath: String,
    addCols: String, alPath: String, colNamesPara: String,
    dfPath: String, predictionCol: String,
     outputPath: String, taskId: String) with HaveBinProbability {

}
