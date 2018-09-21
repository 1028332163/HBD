package com.esoft.dae.preHandle

import com.esoft.dae.dao.BaseDao
import com.esoft.dae.util.handleUtil
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.sql.{SaveMode, Row, SQLContext}
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization._


/**
  *
  * @author liuzw
  *
  *         标准化
  *
  */

class standardScale(taskId: String) extends BaseDao(taskId) {
  def standardScale(sc: SparkContext, basePath: String, inputPath: String,
                    colNamesPara: String, meanScalePara: String, saveOriColPara: String, stdScalePara: String,
                    outputPath: String, taskId: String) {
    ///////构建yarn-spark,从parquet中读取文件
    val sqlContext = new SQLContext(sc)
    val dataFrame = sqlContext.read.parquet(basePath + inputPath)
    //    val dataFrame = csvUtil.readCsv(sc, sqlContext, basePath + inputPath)
    viewDf(dataFrame, "dataFrame")
    ////////////////////参数处理
    val saveOriCol = saveOriColPara.toBoolean
    val meanScale = meanScalePara.toBoolean
    val stdScale = stdScalePara.toBoolean
    //向量features中的列顺序和colNamesPara中的顺序不一致可能会导致最后的df列和列名不对应
    val colNames = handleUtil.sortColNames(colNamesPara)
    val dfCols = dataFrame.columns
    val featureIndexs =
      for (col <- dfCols.indices if colNames.contains(dfCols(col))) yield col
    val SCALE_FEATURE_NUM = featureIndexs.size
    //根据saveOriCol处理head
    val finalHead = if (!saveOriCol) {
      //将要标准化的列从head中除去
      val headArr = handleUtil.getHeadContent(dataFrame.dtypes).split(",").filter {
        colUnit => !colNames.contains(colUnit.split(" ")(0))
      }
      headArr.mkString(",") +
        colNames.map { colName => "," + colName + " double" }.mkString("")
    } else {
      handleUtil.getHeadContent(dataFrame.dtypes) +
        colNames.map { colName => ",std_" + colName + " double" }.mkString("")
    }
    logger.info(finalHead)
    val dfToUse = handleUtil.combineCol(dataFrame, colNamesPara)
    ///////////////////////////////////////标准化处理
    val scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("std_features")
      .setWithStd(stdScale)
      .setWithMean(meanScale)
    // Compute summary statistics by fitting the StandardScaler.
    val scaleModel = scaler.fit(dfToUse)
    val scaledData = scaleModel.transform(dfToUse)
    scaledData.show(false)
    ////////////////////////////对标准化结果向量进行拆解
    val finalSchema = handleUtil.getSchema(finalHead)
    val finalRdd = scaledData.map {
      row =>
        //先将前面的除了最后的两个向量之外的值放到row中
        val originCols = if (saveOriCol) {
          for (elem <- 0 until row.size - 2) yield row.get(elem)
        }
        else {
          for (elem <- 0 until row.size - 2 if !featureIndexs.contains(elem)) yield row.get(elem)
        }
        //取出最后一列的值即标准化之后的值，并对其进行拆解
        val scaledFeaturesCol = row(row.size - 1).asInstanceOf[DenseVector]
        var result = originCols
        for (i <- 0 until SCALE_FEATURE_NUM) {
          result = result :+ scaledFeaturesCol(i)
        }
        Row.fromSeq(result)
    }
    val finalCols =
      if (!saveOriCol)
        dataFrame.columns
      else
        getFinalCols(dataFrame.columns,colNames)
    val finalDf = sqlContext.createDataFrame(finalRdd, finalSchema).select(finalCols.head,finalCols.tail:_*)
    finalDf.show()
    ///////////////////////页面json
    implicit lazy val formats = Serialization.formats(NoTypeHints)
    val colInfos = for (i <- colNames.indices)
      yield ColInfoVO(colNames(i), scaleModel.mean(i), scaleModel.std(i))
    val json = write(colInfos)
    logger.info(json)
    println(json)
    /////////写入数据 并更新数据库状态
    finalDf.write.format("parquet").mode(SaveMode.Overwrite).save(basePath + outputPath)
    super.flagSparked(taskId.toInt, outputPath, finalHead, json)
  }

  case class ColInfoVO(colName: String, mean: Double, std: Double)
  def getFinalCols(oriCols: Array[String], feaCols: Array[String]): Array[String] = {
    val finalCols = new Array[String](oriCols.length + feaCols.length)
    var index = 0
    for (i <- oriCols.indices) {
      val colName = oriCols(i)
      finalCols(index) = colName
      index = index + 1
      if (feaCols.contains(colName)) {
        finalCols(index) = "std_" + colName
        index = index + 1
      }
    }
    finalCols
  }
}

object standardScale {

  def main(args: Array[String]) {


    val basePath = ""
    val inputPath = "data/tree.csv"

    val colNamesPara = "col2,col1"
    val meanScale = "true" //是否减去均值
    val saveOriCol = "true"
    val stdScale = "true" //是否除以标准差


    val outputPath = ""
    val taskId = "182"

    //    val args = Array(basePath, inputPath,
    //      colNamesPara, meanScale, saveOriCol, stdScale, head,
    //      outputPath, taskId)
    //    val sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local"))
    //    exec(sc, args)


    exec(handleUtil.getContext("standardScale"), args)

  }


  def exec(sc: SparkContext, args: Array[String]): Unit = {
    implicit lazy val formats = org.json4s.jackson.Serialization.formats(org.json4s.NoTypeHints)
    case class ArgVO(inputPath: String,
                     featuresCol: String, meanScale: String, saveOriCol: String, stdScale: String,
                     outputPath: String)

    val executer = new standardScale(args.last)
    try {
      args.foreach { x => executer.logger.info("inArg-" + x) }
      val basePath = args(0)
      val ArgVO(inputPath: String,
      colNamesPara: String, meanScalePara: String, saveOriColPara: String, stdScalePara: String,
      outputPath: String)
      = org.json4s.jackson.Serialization.read[ArgVO](args(1))
      val taskId = args(2)
      //executer.checkArgsNum(9, args.length)
      executer.standardScale(sc: SparkContext, basePath: String, inputPath: String,
        colNamesPara: String, meanScalePara: String, saveOriColPara: String, stdScalePara: String,
        outputPath: String, taskId: String)
    } catch {
      case ex: Throwable => executer.handleException(args(args.length - 1).toInt, ex, "standardScale")
    }
  }

}
