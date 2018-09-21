package com.esoft.dae.model

import com.esoft.dae.dao.BaseDao
import com.esoft.dae.util.{ConstantInfo, csvUtil, handleUtil}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.sql.types.StructType
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.ml.regression.{LinearRegressionModel, LinearRegression}
import org.apache.spark.sql.{SaveMode, SQLContext, Row}
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization._

import scala.collection.immutable.IndexedSeq

/**
  * @author
  */
class lineR(taskId: String) extends BaseDao(taskId) {
  def execLinearRegression(sc: SparkContext, basePath: String, inputPath: String,
                           elasticNetParam: String, featuresCol: String, fitIntercept: String,
                           labelCol: String, maxIter: String, alPath: String,
                           predictionCol: String, regParam: String, solver: String, standardization: String, tol: String, weightCol: String,
                           outputPath: String, taskId: String): Unit = {
    /////////构建yarn-spark,从parquet中读取文件
    val sqlContext = new SQLContext(sc)
    val dataFrame = handleUtil.fillLabelCol(sqlContext.read.parquet(basePath + inputPath), labelCol)
    //    val dataFrame = handleUtil.fillLabelCol(csvUtil.readCobCsv(sc, sqlContext, basePath + inputPath, "row int,col1 int,col2 double,label double"),labelCol)
    //////////////////参数处理
    val modelSavePath = ConstantInfo.preModelPath + alPath
    val dfToUse = handleUtil.combineCol(dataFrame, featuresCol) //可以将输入的多个特征列放到fetures向量中
    dfToUse.show(false)
    ////////////////////将算法作用于数据集
    val linearRegression = new LinearRegression()
      .setFeaturesCol("features")
      .setLabelCol(labelCol)
      .setFitIntercept(fitIntercept.toBoolean)
      .setMaxIter(maxIter.toInt)
      .setPredictionCol(predictionCol)
      .setRegParam(regParam.toDouble)
      .setSolver(solver)
      .setStandardization(standardization.toBoolean)
      .setTol(tol.toDouble)
      .setElasticNetParam(elasticNetParam.toDouble)
    if (!"".equals(weightCol)) {
      linearRegression.setWeightCol(weightCol)
    }
    //
    val fitModel: LinearRegressionModel = linearRegression.fit(dfToUse)

    //        val finalDf = fitModel.transform(dfToUse)
    //    //    finalDf.show(false)
    //    val rmse: Double = new RegressionEvaluator()
    //      .setLabelCol(labelCol)
    //      .setPredictionCol(predictionCol)
    //      .setMetricName("rmse")
    //      .evaluate(finalDf)

    //    ////////////////////页面展现用的json
    //每一列的系数
    val colNames = handleUtil.sortColNames(featuresCol)
    val summary = fitModel.summary
    val intercept = fitModel.intercept
    val values = fitModel.coefficients
    val tscores =
      try {
        summary.tValues
      } catch {
        case ex: Exception => new Array[Double](colNames.length + 1)
      }
    val pvalues =
      try {
        summary.pValues
      } catch {
        case ex: Exception => new Array[Double](colNames.length + 1)
      }
    val infoArr =
      ((for (i <- colNames.indices) yield Array(colNames(i), values(i), tscores(i), pvalues(i)))
        :+ Array("intercept", intercept, tscores(colNames.length), pvalues(colNames.length)))
        .map(row => Row.fromSeq(row))

    val showHead = handleUtil.getSchema("colName string,coefficient double,tScore double,pValues double")
    val showDf = sqlContext.createDataFrame(sc.parallelize(infoArr), showHead)
    println(ONLY_ONE_TABLE + "***********")
    showDf.show()
    //mae,mse等值
    val summarySchema = handleUtil.getSchema("summaryType string,value string")
    val summaryRows = Array(Row("MAE", summary.meanAbsoluteError.toString),
      Row("MSE", summary.meanSquaredError.toString), Row("RMSE", summary.rootMeanSquaredError.toString),
      Row("explainedVariance", summary.explainedVariance.toString), Row("r2", summary.r2.toString),
      Row("totalIterations", summary.totalIterations.toString), Row("numInstance", summary.numInstances.toString)
    )
    val summaryDf = sqlContext.createDataFrame(sc.parallelize(summaryRows), summarySchema)
    println(TWO_TABLE_LEFT + "***********")
    summaryDf.show(false)
    //残差label-predictionCol
    val residualsDf = handleUtil.indexDataFrame(summary.residuals)
    println(TWO_TABLE_RIGHT + "***********")
    residualsDf.show(false)
    //残差值的最小值和最大值
    val describe = residualsDf.describe("residuals").collect()
    val min = describe(3)(1).toString
    val max = describe(4)(1).toString

    val vo = SummaryVO(min, max)
    implicit lazy val formats = Serialization.formats(NoTypeHints)
    val json = write(vo)
    println(json)


    //    ////////////////////保存结果及保存json
    val showDfPath = handleUtil.getDfShowPath(outputPath) + "1.parquet"
//    logger.info(showDfPath)
    showDf.write.format("parquet")
      .mode(SaveMode.Overwrite)
      .save(basePath + showDfPath)
    updateTaskInsData(taskId.toInt, showDfPath, showDf.dtypes, ONLY_ONE_TABLE, "no json")

    val summaryDfPath = handleUtil.getDfShowPath(outputPath) + "2.parquet"
    summaryDf.write.format("parquet")
      .mode(SaveMode.Overwrite)
      .save(basePath + summaryDfPath)
    updateTaskInsData(taskId.toInt, summaryDfPath, summaryDf.dtypes, TWO_TABLE_LEFT, "no json")

    val residualsDfPath = handleUtil.getDfShowPath(outputPath) + "3.parquet"
    residualsDf.write.format("parquet")
      .mode(SaveMode.Overwrite)
      .save(basePath + residualsDfPath)
    updateTaskInsData(taskId.toInt, residualsDfPath, residualsDf.dtypes, TWO_TABLE_RIGHT, "no json")

    fitModel.write.overwrite().save(basePath + modelSavePath)
    super.flagSparked(taskId.toInt, outputPath, "no head", json)


  }

  case class SummaryVO(min: String, max: String)

}

object lineR {

  def main(args: Array[String]): Unit = {
    val basePath = ""
    val inputPath = "testCase/DataSourceParquet/input.parquet"


    val elasticNetParam = "0" //[0,1]的double,L1；L1，L2的混合比例，0代表只是用L2，1代表只是L1
    val featuresCol = "col1,col2" //String;特征列；col1，col2
    val fitIntercept = "true" //boolean;是否拟合截距
    val labelCol = "label" //String;被拟合的列
    val maxIter = "100" //最大迭代次数
    val modelSavePath = "trainModel/lineRegression/0815" //训练好的模型的存储路径
    val predictionCol = "prediction" //String;输出结果的列名
    val regParam = "0" //double;正则系数
    val solver = "auto" //l-bfgs||normal||auto；使用的优化算法
    val standardization = "true" //boolean;是否在拟合之前先标准化
    val tol = "1E-6" //double;拟合成功的标准
    val weightCol = "" //String 权值列名


    val outputPath = "/out/20161206/p1549_1481002352573.parquet"
    val taskId = "182"


    //            val args = Array(basePath, inputPath,
    //              elasticNetParam, featuresCol, fitIntercept, labelCol, maxIter, modelSavePath, predictionCol,
    //              regParam, solver, standardization, tol, weightCol,
    //              outputPath, taskId: String)
    //    val args = csvUtil.handleArg()
    //        val sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local"))
    //        exec(sc,args)
    exec(handleUtil.getContext("lineR"), args)

  }

  def exec(sc: SparkContext, args: Array[String]): Unit = {
    implicit lazy val formats = org.json4s.jackson.Serialization.formats(org.json4s.NoTypeHints)
    case class ArgVO(inputPath: String,
                     elasticNetParam: String, featuresCol: String, fitIntercept: String,
                     labelCol: String, maxIter: String, alPath: String,
                     predictionCol: String, regParam: String, solver: String, standardization: String, tol: String, weightCol: String,
                     outputPath: String)

    val executer = new lineR(args.last)
    try {
      args.foreach { x => executer.logger.info("inArg-" + x) }
      val basePath = args(0)
      val ArgVO(inputPath: String,
      elasticNetParam: String, featuresCol: String, fitIntercept: String,
      labelCol: String, maxIter: String, alPath: String,
      predictionCol: String, regParam: String, solver: String, standardization: String, tol: String, weightCol: String,
      outputPath: String)
      = org.json4s.jackson.Serialization.read[ArgVO](args(1))
      val taskId = args(2)
      //executer.checkArgsNum(16, args.length)
      executer.execLinearRegression(sc: SparkContext, basePath: String, inputPath: String,
        elasticNetParam: String, featuresCol: String, fitIntercept: String,
        labelCol: String, maxIter: String, alPath: String,
        predictionCol: String, regParam: String, solver: String, standardization: String, tol: String, weightCol: String,
        outputPath: String, taskId: String)

    } catch {
      case ex: Throwable => executer.handleException(args(args.length - 1).toInt, ex, "lineR")
    }
  }

}
