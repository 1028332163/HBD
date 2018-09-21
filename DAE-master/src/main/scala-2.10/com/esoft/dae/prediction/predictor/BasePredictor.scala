package com.esoft.dae.prediction.predictor

import java.lang.reflect.Method

import com.esoft.dae.dao.BaseDao
import com.esoft.dae.util.handleUtil
import org.apache.spark.SparkContext
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.Param
import org.apache.spark.ml.util.MLReader
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

/**
  * @author liuzw
  */
class BasePredictor(val sc: SparkContext, val basePath: String,
                    val addCols: String, val alPath: String, val colNamesPara: String,
                    val dfPath: String, val predictionCol: String,
                    val outputPath: String, taskId: String) extends BaseDao(taskId) {
  val sqlContext = setSqlContext()
  val dataFrame = setDataFrame()
  var dfToUse = setDfToUse()
  val transformer = setTransformer()
  val fittedDf = setFittedDf()
  val finalDf = setFinalDf()
  val finalHead = handleUtil.getHeadContent(finalDf.dtypes)

  def run(): Unit = {
    viewDf(dataFrame,"dataFrame")
    viewDf(dfToUse,"dfToUse")
    viewDf(fittedDf,"fittedDf")
    viewDf(finalDf,"finalDf")
    saveShowDf()
    saveFinalDf()
  }

  def setSqlContext(): SQLContext = {
    new SQLContext(sc)
  }

  def setDataFrame(): DataFrame = {
    sqlContext.read.parquet(basePath + dfPath)
    //    csvUtil.readCsv(sc, sqlContext, basePath + dfPath)
  }

  def setDfToUse(): DataFrame = {
    handleUtil.combineCol(dataFrame, colNamesPara)
  }

  /////////
  def setTransformer(): Transformer = {
    val loadMethod: Method = Class.forName(getModel(alPath)).getMethod("read") //根据算法的类型获取相应的读取方法
    val transformer = loadMethod.invoke(null).asInstanceOf[MLReader[Any]].load(basePath + alPath).asInstanceOf[Transformer] //transformer是model的父类
    transformer.set[String](new Param(transformer.uid, "predictionCol", ""), predictionCol)
  }

  def setFittedDf(): DataFrame = {
    transformer.transform(dfToUse)

  }

  def setFinalDf(): DataFrame = {
    val finalCols = dataFrame.columns :+ predictionCol
    fittedDf.select(finalCols.head, finalCols.tail: _*)
  }

  def saveShowDf(): Unit = {
    val showDfCols = if (addCols == "")
      Array("rownum", predictionCol)
    else
      "rownum" +: addCols.split(",") :+ predictionCol
    val showDf = finalDf.select(showDfCols.head, showDfCols.tail: _*)
    showDf.show()
    val showDfPath = handleUtil.getDfShowPath(outputPath) + "1.parquet"
    showDf.write.format("parquet")
      .mode(SaveMode.Overwrite)
      .save(basePath + showDfPath)
    updateTaskInsData(taskId.toInt, showDfPath, showDf.dtypes, ONLY_ONE_TABLE, "no json")
  }

  def saveFinalDf(): Unit = {
    finalDf.write.format("parquet")
      .mode(SaveMode.Overwrite)
      .save(basePath + outputPath)
    super.flagSparked(taskId.toInt, outputPath, finalHead, "no json")
  }
}
