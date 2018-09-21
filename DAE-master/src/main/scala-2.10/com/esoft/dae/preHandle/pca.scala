package com.esoft.dae.preHandle

import com.esoft.dae.dao.BaseDao
import com.esoft.dae.util.{handleUtil, csvUtil}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.ml.feature.PCA
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.sql.{SQLContext, _}
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

class pca(taskId: String) extends BaseDao(taskId) {
  /////api里面没有得到特征向量的方法，或许可以尝试使用mllib中的SVD
  def PCAmethod(sc: SparkContext, basePath: String, inputPath: String,
                colNamesPara: String, kPara: String,
                outputPath: String, taskId: String) {
    val sqlContext = new SQLContext(sc)
    val dataFrame = sqlContext.read.parquet(basePath + inputPath)
    //    val dataFrame = csvUtil.readCsv(sc, sqlContext, basePath + inputPath)
    viewDf(dataFrame, "dataFrame")

    ///////////////////////////参数处理
    val k = kPara.toInt
    assert(k <= dataFrame.columns.length, "pca后的特征数需要小于等于用于pca的特征数")
    //根据k的大小构建新的表头 col1 double,col2 double -》col1 double,col2 double,PCA_1 double
    val headTail = new StringBuilder
    for (i <- 0 until k) {
      headTail.append(",pca_").append(i + 1).append(" double")
    }
    val head = handleUtil.getHeadContent(dataFrame.dtypes) + headTail
    val dfToPCA = handleUtil.combineCol(dataFrame, colNamesPara)
    ///////////////////////////调用API进行数据处理
    val pcaModel = new PCA()
      .setInputCol("features")
      .setOutputCol("pcaFeatures")
      .setK(k)
      .fit(dfToPCA)
    val pcaDF = pcaModel.transform(dfToPCA)
    pcaDF.show(false)
    //////////////将最后一列向量pcaFeatures拆解，并将自己构建的features删除
    val finalRdd = pcaDF.map { row =>
      val originCols = for (elem <- 0 until row.size - 2) yield row.get(elem) //将前面的原来的的列放入
    //将最后一列拆解
    val pcaFeaturesCol = row(row.size - 1).asInstanceOf[DenseVector]
      var result = originCols
      for (i <- 0 until k) {
        result = result :+ pcaFeaturesCol(i)
      }
      Row.fromSeq(result)
    }
    val finalHead = handleUtil.getSchema(head)
    val finalDf = sqlContext.createDataFrame(finalRdd, finalHead)
    finalDf.show()
    val cols = for (i <- 0 until k) yield "pca_" + (i + 1)
    val json = handleUtil.getMatrixJson(cols.toArray, handleUtil.sortColNames(colNamesPara), pcaModel.pc.values.map(_.toString))
    println(json)

    //    更新数据库状态
    finalDf.write.format("parquet").mode(SaveMode.Overwrite).save(basePath + outputPath)
    super.flagSparked(taskId.toInt, outputPath, head, json)

  }

}

object pca {

  def main(args: Array[String]) {
    val basePath = ""
    val inputPath = "data/tree.csv"

    //算法参数
    val colNamesPara = "col1,col2,label"
    val kPara = "3"


    val outputPath = "out1/lzw_standardScale_Out.parquet"
    val taskID = "815"



    //    val args = Array(basePath, inputPath, colNamesPara, kPara, head, outputPath, taskID)
    exec(handleUtil.getContext("pca"), args)

  }

  def exec(sc: SparkContext, args: Array[String]): Unit = {
    implicit lazy val formats = org.json4s.jackson.Serialization.formats(org.json4s.NoTypeHints)
    case class ArgVO(inputPath: String,
                     featuresCol: String, kPara: String,
                outputPath: String)

    val executer = new pca(args.last)
    try {
      args.foreach { x => executer.logger.info("inArg-" + x) }
      val basePath = args(0)
      val ArgVO(inputPath: String,
                colNamesPara: String, kPara: String,
                outputPath: String)
      = org.json4s.jackson.Serialization.read[ArgVO](args(1))
      val taskId = args(2)
      //executer.checkArgsNum(7, args.length)
      executer.PCAmethod(sc: SparkContext, basePath: String, inputPath: String,
                colNamesPara: String, kPara: String,
                outputPath: String, taskId: String)
    } catch {
      case ex: Throwable => executer.handleException(args(args.length - 1).toInt, ex, "pca")
    }
  }

}
