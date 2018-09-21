package com.esoft.dae.prediction.predictor

import org.apache.spark.SparkContext
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.classification.GBTClassificationModel

/**
  * Created by asus on 2016/12/23.
  */
class GbdtClsPredictor(sc: SparkContext, basePath: String,
                       addCols: String, alPath: String, colNamesPara: String,
                       dfPath: String, predictionCol: String,
                        outputPath: String, taskId: String)
  extends BasePredictor(sc: SparkContext, basePath: String,
    addCols: String, alPath: String, colNamesPara: String,
    dfPath: String, predictionCol: String,
     outputPath: String, taskId: String) with ShouldIndexFeature with HaveBinProbability {

  override def setTransformer(): Transformer = {
    sc.objectFile[GBTClassificationModel](basePath + alPath + "/model").first()
  }

}
