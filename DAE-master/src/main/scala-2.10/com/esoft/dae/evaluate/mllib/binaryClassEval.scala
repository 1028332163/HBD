package com.esoft.dae.evaluate.mllib

import com.esoft.dae.dao.BaseDao
import com.esoft.dae.util.handleUtil
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.mllib.evaluation.{MulticlassMetrics, BinaryClassificationMetrics}
import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, Row, SQLContext}
import org.apache.spark.SparkContext

/**
  * Created by asus on 2016/12/9.
  */
class binaryClassEval(taskId: String) extends BaseDao(taskId) {
  def binaryClassEvaluate(sc: SparkContext, basePath: String, inputPath: String,
                          probabilityCol: String, predictionCol: String, labelCol: String,
                          outputPath: String, taskId: String): Unit = {
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
    assert(indexcerModel.labels.length < 3, "二分类的标签列的可取值最多为二")
    val labelIndexedDf = indexcerModel.transform(perfectDf)
    indexcerModel.setInputCol(predictionCol).setOutputCol(predictionCol + "_indexed")
    val dfToEval = indexcerModel.transform(labelIndexedDf)
    viewDf(dfToEval, "dfToEval")
    //多分类的混淆矩阵
    val rddToMultiEval = dfToEval.select(predictionCol + "_indexed", labelCol + "_indexed")
      .map(row => (row(0).toString.toDouble, row(1).toString.toDouble))
    val indexerLabel: Array[String] = indexcerModel.labels
    val evaluator: MulticlassMetrics = new MulticlassMetrics(rddToMultiEval)

    val confusionMatrix: Matrix = evaluator.confusionMatrix
    assert(confusionMatrix.toArray.length != 0, "不能将prediction和label进行对应，可能预测列或者标签列选错")
    val json = mutiClassEval.getConfusionMatrixJson(confusionMatrix, indexerLabel)
    logger.info(json)
    //二分类的ROC信息
    val rddToBinEval: RDD[(Double, Double)] = dfToEval.select(probabilityCol, labelCol + "_indexed")
      .map(row => (row(0).toString.toDouble, row(1).toString.toDouble))
    val rocDf = getBinEvalDf(sqlContext, rddToBinEval)
    viewDf(rocDf, "rocDf")
    //    //////////结果保存
    val finalDfPath = handleUtil.getDfShowPath(outputPath) + "1.parquet"
    rocDf.write.format("parquet")
      .mode(SaveMode.Overwrite)
      .save(basePath + finalDfPath)
    updateTaskInsData(taskId.toInt, finalDfPath, rocDf.dtypes, CHART_JSON + "_ROC", "no json")

    super.flagSparked(taskId.toInt, outputPath, "no head", json)
  }

  def getBinEvalDf(sqlContext: SQLContext, rddToUse: RDD[(Double, Double)]): DataFrame = {

    val evaluator = new BinaryClassificationMetrics(rddToUse)
    val nf = java.text.NumberFormat.getInstance()
    nf.setGroupingUsed(false)
    nf.setMaximumFractionDigits(10)
    logger.info("evalResultNum:"+evaluator.precisionByThreshold.count())
    val precisionThresh =
      handleUtil.decDfSize(sqlContext.createDataFrame(
        evaluator.precisionByThreshold.map(row => Row(nf.format(row._1), nf.format(row._2))),
        handleUtil.getSchema("threshold string,precision string")))
    viewDf(precisionThresh, "precisionThresh")

    val recallThresh =
      handleUtil.decDfSize(sqlContext.createDataFrame(
        evaluator.recallByThreshold().map(row => Row(nf.format(row._1), nf.format(row._2))),
        handleUtil.getSchema("threshold string,recall string")))
    viewDf(recallThresh, "recallThresh")

    val fMeasureThresh =
      handleUtil.decDfSize(sqlContext.createDataFrame(
        evaluator.fMeasureByThreshold().map(row => Row(nf.format(row._1), nf.format(row._2))),
        handleUtil.getSchema("threshold string,fmeasure string")))
    viewDf(fMeasureThresh, "fMeasureThresh")

    val rocRdd = evaluator.roc
    val rocDataSize = rocRdd.count()
    val roc =
      handleUtil.decDfSize(sqlContext.createDataFrame(
        rocRdd.zipWithIndex().filter(row => row._2 != 0 && row._2 != rocDataSize - 1)
          .map(row => Row(nf.format(row._1._1), nf.format(row._1._2))),
        handleUtil.getSchema("fpr string,tpr string")))
    viewDf(roc, "rocDf")

    roc.join(recallThresh, "rownum").drop("threshold")
      .join(precisionThresh, "rownum").drop("threshold")
      .join(fMeasureThresh, "rownum").sort("rownum")

  }

}

object binaryClassEval {

  def main(args: Array[String]) {
    exec(handleUtil.getContext("binaryClassEval"), args)
  }

  def exec(sc: SparkContext, args: Array[String]): Unit = {
    implicit lazy val formats = org.json4s.jackson.Serialization.formats(org.json4s.NoTypeHints)
    case class ArgVO(inputPath: String,
                     probabilityCol: String, predictionCol: String, labelCol: String,
                     outputPath: String)

    val executer = new binaryClassEval(args.last)
    try {
      args.foreach { x => executer.logger.info("inArg-" + x) }
      val basePath = args(0)
      val ArgVO(inputPath: String,
      probabilityCol: String, predictionCol: String, labelCol: String,
      outputPath: String)
      = org.json4s.jackson.Serialization.read[ArgVO](args(1))
      val taskId = args(2)
      //executer.checkArgsNum(6, args.length)
      executer.binaryClassEvaluate(sc: SparkContext, basePath: String, inputPath: String,
        probabilityCol: String, predictionCol: String, labelCol: String,
        outputPath: String, taskId: String)

    } catch {
      case ex: Throwable => executer.handleException(args(args.length - 1).toInt, ex, "binaryClassEval")
    }
  }

}


//val perfectDf = dataFrame.na.drop(Array(probabilityCol, predictionCol, labelCol))
//val preGroupDf = perfectDf.select(probabilityCol, predictionCol)
//.groupBy(predictionCol).min().toDF("prediction", "minProb")
//assert(preGroupDf.count() < 3, "预测列中的枚举个数大于2")
//val labelGroupDf = perfectDf.select(labelCol).groupBy(labelCol).min()
//assert(labelGroupDf.count() < 3, "标签列中的枚举个数大于2")
//assert(preGroupDf.select(predictionCol).collect().map(row => row.getString(0)).toSet.equals(
//labelGroupDf.select(labelCol).collect().map(row => row.get(0).toString).toSet),
//"预测列和标签列的枚举值不一样"
//)
//
//val ascArr = preGroupDf.sort("minProb").collect().map(row => row.getString(0))//0-1对应的label
//val preTranMap = (for (i <- ascArr.indices) yield (ascArr(i), i.toString)).toMap
//println(preTranMap)
//val dfToEval =
//if ("StringType".equals(dataFrame.dtypes.toMap.apply(labelCol)))
////label和prediction都是string类型,可以使用相同的替换map
//perfectDf.na.replace(Array(predictionCol, labelCol), preTranMap)
//else
////label和prediction类型不一样,map的类型不能一样
//perfectDf.na.replace(predictionCol,preTranMap)
//.na.replace(labelCol,preTranMap.map(tuple => (tuple._1.toDouble, tuple._2.toDouble)))
//dfToEval.show
