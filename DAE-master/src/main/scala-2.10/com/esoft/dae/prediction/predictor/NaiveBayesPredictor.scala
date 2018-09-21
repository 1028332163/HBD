package com.esoft.dae.prediction.predictor

import java.lang.reflect.Method

import org.apache.spark.SparkContext
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.Param
import org.apache.spark.ml.util.MLReader

/**
  * Created by asus on 2016/12/16.
  */
class NaiveBayesPredictor(sc: SparkContext, basePath: String,
                          addCols: String, alPath: String, colNamesPara: String,
                          dfPath: String, predictionCol: String,
                           outputPath: String, taskId: String)
  extends BasePredictor(sc: SparkContext, basePath: String,
    addCols: String, alPath: String, colNamesPara: String,
    dfPath: String, predictionCol: String,
     outputPath: String, taskId: String) with ShouldIndexFeature with HaveMulProbability {

  override def setTransformer(): Transformer = {
    //需要从savePath+model里面读取
    val loadMethod: Method = Class.forName(getModel(alPath)).getMethod("read") //根据算法的类型获取相应的读取方法
    val transformer = loadMethod.invoke(null).asInstanceOf[MLReader[Any]].load(basePath + alPath + "/model").asInstanceOf[Transformer] //transformer是model的父类
    transformer.set[String](new Param(transformer.uid, "predictionCol", ""), predictionCol)
  }

}
