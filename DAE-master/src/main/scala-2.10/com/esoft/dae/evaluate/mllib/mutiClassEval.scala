package com.esoft.dae.evaluate.mllib

import com.esoft.dae.dao.BaseDao
import com.esoft.dae.util.handleUtil
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.sql.{DataFrame, SaveMode, Row, SQLContext}
import org.apache.spark.SparkContext
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

/**
  * Created by asus on 2016/12/10.
  */
class mutiClassEval(taskId: String) extends BaseDao(taskId) {
  def mutiClassEvaluate(sc: SparkContext, basePath: String, inputPath: String,
                        labelCol: String, predictionCol: String, outputPath: String, taskId: String): Unit = {
    /////////构建yarn-spark,从parquet中读取文件
    val sqlContext = new SQLContext(sc)
    val dataFrame = sqlContext.read.parquet(basePath + inputPath)
    viewDf(dataFrame, "dataFrame")
    //index数据集中的prediction和label
    val perfectDf = dataFrame.na.drop(Array(labelCol, predictionCol))
    val indexcerModel = new StringIndexer()
      .setInputCol(labelCol)
      .setOutputCol(labelCol + "_indexed")
      .setHandleInvalid("skip")
      .fit(perfectDf)
    val labelIndexedDf = indexcerModel.transform(perfectDf)
    indexcerModel.setInputCol(predictionCol).setOutputCol(predictionCol + "_indexed")
    val dfToEval = indexcerModel.transform(labelIndexedDf)
    assert(dfToEval.count>0,"标签列或预测列选错")
    viewDf(dfToEval, "dfToEval")
    //多分类评估
    val rddToMultiEval = dfToEval.select(predictionCol + "_indexed", labelCol + "_indexed")
      .map(row => (row(0).toString.toDouble, row(1).toString.toDouble))
    val indexerLabel: Array[String] = indexcerModel.labels
    val evaluator: MulticlassMetrics = new MulticlassMetrics(rddToMultiEval)

    val labels: Array[Double] = evaluator.labels //混淆矩阵:矩阵按照label的数字升序排列（官方文档说明）
    val confusionMatrix: Matrix = evaluator.confusionMatrix
    assert(confusionMatrix.toArray.length != 0, "不能将prediction和label进行对应，可能预测列或者标签列选错")
    val json = mutiClassEval.getConfusionMatrixJson(confusionMatrix, indexerLabel)
    logger.info(json)

    val summaryDf = getSummaryDf(evaluator, labels, indexerLabel, sc, sqlContext)//评价标准组成的矩阵
    viewDf(summaryDf, "summaryDf")
    //////////结果保存
    val summaryDfPath = handleUtil.getDfShowPath(outputPath) + "1.parquet"
    summaryDf.write.format("parquet")
      .mode(SaveMode.Overwrite)
      .save(basePath + summaryDfPath)
    updateTaskInsData(taskId.toInt, summaryDfPath, summaryDf.dtypes, TWO_TABLE_RIGHT, "no json")
    super.flagSparked(taskId.toInt, outputPath, "no head", json)
  }

  def getSummaryDf(evaluator: MulticlassMetrics,
                   labels: Array[Double], indexerLabel: Array[String],
                   sc: SparkContext, sqlContext: SQLContext): DataFrame = {
    val summaryRows = new Array[Row](labels.length + 1)
    val summarySchema = handleUtil.getSchema("label string,fMeasure string,precision string,recall string,FPR string,TPR string")
    summaryRows(0) =
      Row("total", evaluator.weightedFMeasure.toString, evaluator.weightedPrecision.toString,
        evaluator.weightedRecall.toString, evaluator.weightedFalsePositiveRate.toString, evaluator.weightedTruePositiveRate.toString)
    for (i <- labels.indices) {
      val label = labels(i)
      summaryRows(i + 1) = Row(indexerLabel(label.toInt), evaluator.fMeasure(label).toString, evaluator.precision(label).toString,
        evaluator.recall(label).toString, evaluator.falsePositiveRate(label).toString, evaluator.truePositiveRate(label).toString)
    }
    sqlContext.createDataFrame(sc.parallelize(summaryRows), summarySchema)
  }
}

object mutiClassEval {

  def main(args: Array[String]) {
    exec(handleUtil.getContext("mutiClassEval"), args)
  }


  def exec(sc: SparkContext, args: Array[String]): Unit = {
    implicit lazy val formats = org.json4s.jackson.Serialization.formats(org.json4s.NoTypeHints)
    case class ArgVO(inputPath: String,
                     labelCol: String, predictionCol: String, outputPath: String)

    val executer = new mutiClassEval(args.last)
    try {
      args.foreach { x => executer.logger.info("inArg-" + x) }
      val basePath = args(0)
      val ArgVO(inputPath: String,
      labelCol: String, predictionCol: String, outputPath: String)
      = org.json4s.jackson.Serialization.read[ArgVO](args(1))
      val taskId = args(2)
      //executer.checkArgsNum(6, args.length)
      executer.mutiClassEvaluate(sc: SparkContext, basePath: String, inputPath: String,
        labelCol: String, predictionCol: String, outputPath: String, taskId: String)

    } catch {
      case ex: Throwable => executer.handleException(args(args.length - 1).toInt, ex, "mutiClassEval")
    }
  }

  def getConfusionMatrixJson(confusionMatrix: Matrix, indexerLabel: Array[String]): String = {
    //    confusionMatrix(1)(2)
    var total = 0
    var error = 0
    val confusionRows = (for {i <- Range(0, confusionMatrix.numRows)} yield {
      var rowTotal = 0
      var rowError = 0
      for (j <- Range(0, confusionMatrix.numCols)) {
        rowTotal += confusionMatrix(i, j).toInt
        if (i != j)
          rowError += confusionMatrix(i, j).toInt
      }
      total += rowTotal
      error += rowError
      (for (j <- Range(0, confusionMatrix.numCols))
        yield confusionMatrix(i, j).toInt.toString).toArray :+ (rowError.toDouble / rowTotal).toString :+ (rowError + "/" + rowTotal)
    }).toArray.flatten
    //计算最后一行数据
    val lastRow = (for (j <- Range(0, confusionMatrix.numCols)) yield {
      var sum = 0
      for (i <- Range(0, confusionMatrix.numRows)) {
        sum = sum + confusionMatrix(i, j).toInt
      }
      sum.toString
    }) :+ (error.toDouble / total).toString :+ (error + "/" + total)
    val data = confusionRows ++ lastRow
    implicit lazy val formats = Serialization.formats(NoTypeHints)
    Serialization.write(Table(indexerLabel :+ "Error" :+ "Rate", indexerLabel :+ "Total", data))
  }

  case class Table(colNames: Array[String], rowNames: Array[String], data: Array[String])

}
