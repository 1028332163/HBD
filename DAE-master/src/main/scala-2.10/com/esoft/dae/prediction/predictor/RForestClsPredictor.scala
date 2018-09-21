package com.esoft.dae.prediction.predictor

import org.apache.spark.SparkContext
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.classification.RandomForestClassificationModel

/**
  * Created by asus on 2016/12/14.
  */
class RForestClsPredictor(sc: SparkContext, basePath: String,
                          addCols: String, alPath: String, colNamesPara: String,
                          dfPath: String, predictionCol: String,
                           outputPath: String, taskId: String)
  extends BasePredictor(sc: SparkContext, basePath: String,
    addCols: String, alPath: String, colNamesPara: String,
    dfPath: String, predictionCol: String,
     outputPath: String, taskId: String) with ShouldIndexFeature with HaveMulProbability{

  override def setTransformer(): Transformer = {
    sc.objectFile[RandomForestClassificationModel](basePath + alPath + "/model").first()
  }

}
