package com.esoft.dae.preHandle

import com.esoft.dae.dao.BaseDao
import com.esoft.dae.util.{handleUtil, csvUtil}
import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.sql.{SaveMode, Row, SQLContext}
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.immutable.IndexedSeq


/**
  *
  * @author liuzw
  *
  *         归一化
  *
  */
class minMaxScale(taskId: String) extends BaseDao(taskId) {
  def minMaxScale(sc: SparkContext, basePath: String, inputPath: String,
                  colNamesPara: String, max: String, min: String, saveOriColPara: String,
                  outputPath: String, taskId: String) {


    //        ///////构建yarn-spark,从parquet中读取文件
    val sqlContext = new SQLContext(sc)
    val dataFrame = sqlContext.read.parquet(basePath + inputPath)
    //            val dataFrame = csvUtil.readCsv(sc, sqlContext, basePath + inputPath)
    viewDf(dataFrame, "dataFrame")

    //////////////////////////参数处理
    val saveOriCol = saveOriColPara.toBoolean
    //向量features中的列顺序和colNamesPara中的顺序不一致可能会导致最后的df列和列名不对应
    val colNames = handleUtil.sortColNames(colNamesPara)
    val dfCols: Array[String] = dataFrame.columns
    val featureIndexs: IndexedSeq[Int] =
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
        colNames.map { colName => ",nor_" + colName + " " + "double" }.mkString("")
    }
    val dfToUse = handleUtil.combineCol(dataFrame, colNamesPara)
    println(finalHead)
    //////////////////////////////归一化处理
    val scaler = new MinMaxScaler().setInputCol("features").setOutputCol("nor_features")
      .setMin(min.toDouble).setMax(max.toDouble)
    val scalerModel = scaler.fit(dfToUse)
    // rescale each feature to range [min, max].
    val scaledData = scalerModel.transform(dfToUse)
    scaledData.show()
    ////////////////////////////对归一化结果向量进行拆解
    val finalSchema = handleUtil.getSchema(finalHead)
    val finalRdd = scaledData.rdd.map {
      row =>
        val originCols = if (saveOriCol) {
          //保留原始列
          for (elem <- 0 until row.size - 2) yield row.get(elem)
        }
        else {
          //保留原始列
          for (elem <- 0 until row.size - 2 if !featureIndexs.contains(elem)) yield row.get(elem)
        }
        val scaledFeaturesCol: DenseVector = row(row.size - 1).asInstanceOf[DenseVector]
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
    ///////////////////////////////页面json

    implicit lazy val formats = Serialization.formats(NoTypeHints)
    val colInfos = for (i <- colNames.indices)
      yield ColInfoVO(colNames(i), scalerModel.originalMin(i), scalerModel.originalMax(i))
    val json = Serialization.write(colInfos)
    println(json)
    //      写入数据更新数据库
    finalDf.write.format("parquet").mode(SaveMode.Overwrite).save(basePath + outputPath)
    super.flagSparked(taskId.toInt, outputPath, finalHead, json)

  }

  def getFinalCols(oriCols: Array[String], feaCols: Array[String]): Array[String] = {
    val finalCols = new Array[String](oriCols.length + feaCols.length)
    var index = 0
    for (i <- oriCols.indices) {
      val colName = oriCols(i)
      finalCols(index) = colName
      index = index + 1
      if (feaCols.contains(colName)) {
        finalCols(index) = "nor_" + colName
        index = index + 1
      }
    }
    finalCols
  }

  case class ColInfoVO(colName: String, min: Double, max: Double)

}

object minMaxScale {

  def main(args: Array[String]) {

    val basePath = ""
    val inputPath = "data/tree.csv"

    val colNamesPara = "col2,col1"
    val max = "1" //映射区间的最大值
    val min = "0" //映射区间的最小值
    val saveOriCol = "true"

    val outputPath = "out/1"
    val taskId = "182"

    //    val args = Array(basePath, inputPath,
    //      colNamesPara, max, min, saveOriCol, head,
    //      outputPath, taskId)
    //    val sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local"))
    //    exec(sc,args)

    exec(handleUtil.getContext("minMaxScale"), args)

  }

  def exec(sc: SparkContext, args: Array[String]): Unit = {
    implicit lazy val formats = org.json4s.jackson.Serialization.formats(org.json4s.NoTypeHints)
    case class ArgVO(inputPath: String,
                     featuresCol: String, max: String, min: String, saveOriColPara: String,
                     outputPath: String)

    val executer = new minMaxScale(args.last)
    try {
      args.foreach { x => executer.logger.info("inArg-" + x) }
      val basePath = args(0)
      val ArgVO(inputPath: String,
      colNamesPara: String, max: String, min: String, saveOriColPara: String,
      outputPath: String)
      = org.json4s.jackson.Serialization.read[ArgVO](args(1))
      val taskId = args(2)
      //executer.checkArgsNum(9, args.length)
      executer.minMaxScale(sc: SparkContext, basePath: String, inputPath: String,
        colNamesPara: String, max: String, min: String, saveOriColPara: String,
        outputPath: String, taskId: String)


    } catch {
      case ex: Throwable => executer.handleException(args(args.length - 1).toInt, ex, "minMaxScale")
    }
  }

}
