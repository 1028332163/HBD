package com.esoft.dae.prediction.predictor

import org.apache.spark.SparkContext
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.regression.DecisionTreeRegressionModel

/**
  * Created by asus on 2016/12/14.
  */
class DTreeRgsPredictor(sc: SparkContext, basePath: String,
                        addCols: String, alPath: String, colNamesPara: String,
                        dfPath: String, predictionCol: String,
                         outputPath: String, taskId: String)
  extends BasePredictor(sc: SparkContext, basePath: String,
    addCols: String, alPath: String, colNamesPara: String,
    dfPath: String, predictionCol: String,
     outputPath: String, taskId: String) with ShouldIndexFeature {


  override def setTransformer(): Transformer = {
    sc.objectFile[DecisionTreeRegressionModel](basePath + alPath + "/model").first()
  }
}
