package com.esoft.dae.prediction.predictor

import org.apache.spark.SparkContext
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.regression.GBTRegressionModel

/**
  * Created by asus on 2016/12/23.
  */
class GbdtRgsPredictor(sc: SparkContext, basePath: String,
                       addCols: String, alPath: String, colNamesPara: String,
                       dfPath: String, predictionCol: String,
                        outputPath: String, taskId: String)
  extends BasePredictor(sc: SparkContext, basePath: String,
    addCols: String, alPath: String, colNamesPara: String,
    dfPath: String, predictionCol: String,
     outputPath: String, taskId: String) with ShouldIndexFeature {

  override def setTransformer(): Transformer = {
    sc.objectFile[GBTRegressionModel](basePath + alPath + "/model").first()
  }

}
