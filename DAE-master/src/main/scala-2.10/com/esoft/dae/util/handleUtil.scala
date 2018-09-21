package com.esoft.dae.util

import java.io.{BufferedInputStream, InputStream}
import java.util.Properties

import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.mllib.linalg.{Vectors, VectorUDT}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.mllib.linalg.Vector
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization._

import scala.collection.immutable.IndexedSeq

/**
  * Created by asus on 2016/12/14.
  */
object handleUtil {
  val hiveTypeMap = Map("IntegerType" -> "int", "StringType" -> "string", "DoubleType" -> "double", "TimestampType" -> "timestamp")

  /**
    * @param alPath 算法的路径
    * @return
    **/
  def getAl(alPath: String): String = {
    alPath.split("/")(0)
  }

  //决策树时不会使用原来的特征列，需要根据原来的特征列获取新的特征列列名
  def getClassfierFeatures(features: String, dType: Array[(String, String)]): String = {
    val featureArr = features.split(",")
    val newfeatures = for (i <- dType.indices if featureArr.contains(dType(i)._1)) yield {
      if ("StringType".equals(dType(i)._2)) {
        dType(i)._1 + "_indexed"
      } else {
        dType(i)._1
      }
    }
    newfeatures.mkString(",")
  }

  //将string类型的特征列进行index
  def indexFeatures(oriDf: DataFrame, featuresCol: String, savePath: String): DataFrame = {
    val strFeatures = getStrFeatures(oriDf.dtypes, featuresCol)
    var dataFrame = oriDf
    val indexer = new StringIndexer()
    for (i <- strFeatures.indices) {
      val col = strFeatures(i)
      indexer.setInputCol(col)
        .setOutputCol(col + "_indexed")
      val model = indexer.fit(dataFrame)
      dataFrame = model.transform(dataFrame)
      model.write.overwrite().save(savePath + "/" + col)
    }
    dataFrame
  }

  //得到特征列哪些是字符串类型
  def getStrFeatures(dType: Array[(String, String)], features: String): IndexedSeq[String] = {
    //    StringType
    val featureArr = features.split(",")
    for (i <- dType.indices if "StringType".equals(dType(i)._2) && featureArr.contains(dType(i)._1)) yield dType(i)._1

  }

  //根据任务的finalDf数据集输出路径得到用于页面展现的showDf数据集的路径
  //    "/out/20161206/p1549_1481002352573.parquet"=》"/showDf/20161206/p1549_1481002352573"
  def getDfShowPath(outputPath: String): String = {
    "/showDf" + outputPath.substring(3, outputPath.length - 8)
  }

  //将"a1 b1,a2 b2"转化为([a1,a2],[a2,b2])
  def strToTwoArr(str: String): (Array[String], Array[String]) = {
    val unitArr = str.split(",")
    val prefixArr = new Array[String](unitArr.length)
    val suffixArr = new Array[String](unitArr.length)
    for (i <- unitArr.indices) {
      val oneCol = unitArr(i).split(" ")
      prefixArr(i) = oneCol(0)
      suffixArr(i) = oneCol(1)
    }
    (prefixArr, suffixArr)
  }

  //将"a1 b1,a2 b2"转化为[(a1,b1),(a2,b2)]
  def strToTupleArr(str: String): Array[(String, String)] = {
    val unitArr = str.split(",")
    unitArr.map { unit =>
      val oneCol = unit.split(" ")
      (oneCol(0), oneCol(1))
    }
  }

  def combineCol(dataFrame: DataFrame, colNamesPara: String): DataFrame = {
    val sqlContext: SQLContext = dataFrame.sqlContext
    assert(colNamesPara != "", "未选择特征列")
    val feaNames = sortColNames(colNamesPara)//将特征排序
    val dfCols = dataFrame.columns
    val featureIndexs = getColsIndex(feaNames, dfCols)
    //获取列和均值的对应关系
    val indexToMean: Map[Int, Double] = getIndexToMean(feaNames, dfCols, dataFrame)
    /////////////////使用选定的列来构建向量
    val newSchema: StructType = dataFrame.schema.add(
      StructField("features", new VectorUDT, nullable = true))
    val rddToUse = dataFrame.map { row =>
      val featuresSeq: Array[Double] = featureIndexs.map {
        feaIndex =>
          try {
            row.get(feaIndex).toString.toDouble
          } catch {
            //缺失值使用均值填充
            case ex: Exception => indexToMean(feaIndex)
          }
      }
      val featuresVec = Vectors.dense(featuresSeq.toArray)
      val result = row.toSeq :+ featuresVec
      Row.fromSeq(result)
    }
    sqlContext.createDataFrame(rddToUse, newSchema)
  }

  //根据特征列和label列的列名，将dataframe中的相关数据取出组成rdd[LabelPoint]
  def getLabelPointRdd(dataFrame: DataFrame, colNamesPara: String, labelCol: String): RDD[LabeledPoint] = {
    ///选定数据集的几列构成一个向量
    val feaNames = sortColNames(colNamesPara)
    val dfCols = dataFrame.columns
    val featureIndexs = getColsIndex(feaNames, dfCols)
    val indexToMean = getIndexToMean(feaNames, dfCols, dataFrame)

    val labelIndex = for (col <- dfCols.indices if labelCol.equals(dfCols(col))) yield col
    /////////////////使用选定的列来构建向量
    dataFrame.map { row =>
      val featuresSeq = featureIndexs.map {
        feaIndex =>
          try {
            row.get(feaIndex).toString.toDouble
          } catch {
            case ex: Exception => indexToMean(feaIndex)
          }
      }
      val labelPoint = for (elem <- 0 until row.size if labelIndex.contains(elem)) yield row.get(elem).toString.toDouble
      LabeledPoint(labelPoint(0), Vectors.dense(featuresSeq.toArray))
    }
  }

  //根据featuresCol得到用于mllib中model.run的输入data:RDD[Vector]
  def getVectorRdd(dataFrame: DataFrame, colNamesPara: String): RDD[Vector] = {
    ///选定数据集的几列构成一个向量
    val feaNames = sortColNames(colNamesPara)
    val dfCols = dataFrame.columns
    val featureIndexs = getColsIndex(feaNames, dfCols)
    val indexToMean = getIndexToMean(feaNames, dfCols, dataFrame)
    /////////////////使用选定的列来构建向量
    dataFrame.map { row =>
      val featuresSeq = featureIndexs.map {
        feaIndex =>
          try {
            row.get(feaIndex).toString.toDouble
          } catch {
            case ex: Exception => indexToMean(feaIndex)
          }
      }
      Vectors.dense(featuresSeq.toArray)
    }
  }

  //将列名的数组转换为列名在列在df的角标的数组
  //feaNames:[col1,col3]   dfCols:[col1,col2,col3] => [0,2]
  def getColsIndex(feaNames: Array[String], dfCols: Array[String]): Array[Int] = {
    (for (i <- feaNames.indices) yield {
      var feaIndex: Int = 0
      val feaName = feaNames(i)
      for (j <- dfCols.indices) {
        if (feaName == dfCols(j)) {
          feaIndex = j
        }
      }
      feaIndex
    }).toArray
  }

  //获取feaNames中的col的index->mean的Map
  def getIndexToMean(feaNames: Array[String], dfCols: Array[String], dataFrame: DataFrame): Map[Int, Double] = {
    feaNames.map {
      colName =>
        val index = dfCols.indexOf(colName)
        val perfectDf = dataFrame.na.drop(Array(colName)) //此列没有缺失值的dataframe
      val mean =
        if (perfectDf.count == 0) {
          0
        } else {
          try {
            //用户选择string特征列后就会导致mean方法的空指针
            perfectDf.select(functions.mean(colName)).first.get(0).toString.toDouble
          } catch {
            case ex: Throwable =>
              throw new Exception("string类型的列不能作为此算法的特征列")
          }

        }
        (index, mean)
    }.toMap //目录=》均值
  }

  //将传入的col3，col2，col1按照"字符串排序规则"重新排列成col1，col2，col3
  def sortColNames(colNamesPara: String): Array[String] = {
    colNamesPara.split(",").sorted
  }

  //获取feaNames中的col的colName->mean的Map
  def getColToMean(feaNames: Array[String], dfCols: Array[String], dataFrame: DataFrame): Map[String, Double] = {
    feaNames.map {
      colName =>
        val perfectDf = dataFrame.na.drop(Array(colName)) //此列没有缺失值的dataframe
      val mean =
        if (perfectDf.count == 0) {
          0
        } else {
          try {
            //用户选择string特征列后就会导致mean方法的空指针
            perfectDf.select(functions.mean(colName)).first.get(0).toString.toDouble
          } catch {
            case ex: Throwable =>
              throw new Exception("string类型的列不能作为此算法的特征列")
          }

        }
        (colName, mean)
    }.toMap //列名 =》均值
  }

  def sortColNames(colNamesPara: String, dfCols: Array[String]): Array[String] = {
    val colNames = colNamesPara.split(",")
    (for (i <- dfCols.indices if colNames.contains(dfCols(i))) yield dfCols(i)).toArray
  }

  //mllib包专用，predict之后，将产生的新的prediction与原来的dataframe合并
  def getFinalDf(oriDf: DataFrame, prediciton: DataFrame): DataFrame = {
    val sqlContext = oriDf.sqlContext
    val zipRddLeft = oriDf.rdd.zipWithIndex().map(row => (row._2, row._1))
    val zipRddRight = prediciton.rdd.zipWithIndex().map(row => (row._2, row._1))

    val finalRdd = zipRddLeft.join(zipRddRight).map { row =>
      val data = row._2
      Row.fromSeq(data._1.toSeq ++: data._2.toSeq)
    }
    sqlContext.createDataFrame(finalRdd, StructType(oriDf.schema.toSeq ++: prediciton.schema.toSeq))
  }

  def getSymMatrixJson(cols: Array[String], matrix: Array[Array[String]]): String = {
    //获得n*n矩阵的json
    case class ShowJson(numCount: Int, rows: IndexedSeq[ShowRow])
    case class ShowCol(cols: String, Proportion: Int)
    case class ShowRow(value: Array[String], data: Array[ShowCol])
    val data = cols.map(col => new ShowCol(col, 1))
    val indexLen = matrix.length - 1
    val rows: IndexedSeq[ShowRow] = for (i <- matrix.indices) yield ShowRow(matrix(indexLen-i), data)
    implicit lazy val formats = Serialization.formats(NoTypeHints)
    write(ShowJson(Set(matrix.flatten: _*).size, rows))
  }

  def getMatrixJson(cols: Array[String], rows: Array[String], data: Array[String]): String = {
    //获得m*n矩阵的json
    implicit lazy val formats = Serialization.formats(NoTypeHints)
    case class ShowCol(name: String, profit: Int)
    case class ShowRow(name: String, cols: Array[ShowCol])
    val axis = Serialization.write(rows.map(ShowRow(_, cols.map(ShowCol(_, 1)))))
    case class FinalVO(axis: String, data: Array[String])
    Serialization.write(FinalVO(axis, data))

  }

  def fillLabelCol(df: DataFrame, labelCol: String): DataFrame = {
    try {
      val colMean = df.select(labelCol).na.drop()
        .select(functions.mean(labelCol)).first.get(0).toString.toDouble
      df.na.fill(colMean, Array(labelCol))
    } catch {
      case ex: Exception => throw new Exception("标签列全部为空值")
    }

  }

  //  def getColFea(dataFrame:DataFrame,feaType:String): String ={
  //    val feaCol = dataFrame.map(row=>row(1)).collect()
  //    feaType match {
  //      case "count" =>feaCol(0).toString
  //      case "mean" =>feaCol(1).toString
  //      case "stddev" =>feaCol(2).toString
  //      case "min"=>feaCol(3).toString
  //      case "max"=>feaCol(4).toString
  //    }
  //  }
  def getContext(name: String): SparkContext = {
    val sparkModel = loadProperties("spark.model")
    val conf = new SparkConf().setAppName(name)
    if (sparkModel == "local") {
      conf.setMaster("local[4]")
    }
    new SparkContext(conf)
  }

  def loadProperties(key: String): String = {
    val properties = new Properties()
    val in: InputStream = getClass.getResourceAsStream("/db.properties")
    properties.load(new BufferedInputStream(in))
    properties.getProperty(key)
  }

  /**
    * 将df间隔抽样到指定的大小
    *
    * @param oriDf     ：被抽样的df
    * @param finalSize : 最后输出的df大小要为多大
    * @return
    */
  def decDfSize(oriDf: DataFrame, finalSize: Double = 200): DataFrame = {
    val oriSize = oriDf.count()
    if (oriSize <= finalSize) {
      oriDf.persist()
      indexDataFrame(oriDf)//索引数据集
    } else {
      val indexedDf = indexDataFrame(oriDf)
      val gap: Double = oriSize / finalSize//采样间隔
      val shouldOut: UserDefinedFunction = udf(
        (index: Double) => {
          val outNum: Double = (index / gap).toInt * gap
          outNum match {
            case out if isRound(outNum,index)||isRound(outNum+gap,index) => true
            case _ => false
          }
        })
      indexedDf.filter(shouldOut(col("rownum")))
    }
  }
  def isRound(f:Double,i:Double): Boolean ={
    f >= i - 0.5 && f < i + 0.5
  }
  /**
    * 根据index过滤dataFrame
    *
    * @param oriDf        被过滤的Df
    * @param leftIndexArr 留下的行的index
    * @param startIndex   df的角标是从1开始还是从零开始
    * @return
    */
  def filteByIndex(oriDf: DataFrame, leftIndexArr: Seq[Long], startIndex: Int = 0): DataFrame = {
    val indexedDf = indexDataFrame(oriDf, startIndex)
    val shouldOut: UserDefinedFunction = udf(
      (index: Double) => if (leftIndexArr.contains(index)) true
      else false
    )
    indexedDf.filter(shouldOut(col("rownum"))).drop("rownum")
  }

  def getCol(dataFrame: DataFrame): Array[Any] = {
    dataFrame.collect().map(row => row.get(0))
  }

  //为dataframe添加索引列
  def indexDataFrame(dataFramePara: DataFrame, start: Int = 1): DataFrame = {
    val sqlContext = dataFramePara.sqlContext
    val dataFrame = dataFramePara.drop("rownum")
    val finalRdd = dataFrame.rdd.zipWithIndex().map {
      case (oriRow, rowNum) => Row.fromSeq((rowNum.toInt + start) +: oriRow.toSeq)
    }
    sqlContext.createDataFrame(finalRdd,
      handleUtil.getSchema("rownum int," + handleUtil.getHeadContent(dataFrame.dtypes)))
  }

  def getSchema(head: String): StructType = {
    StructType(head.split(",")
      .map {
        filedName =>
          StructField(filedName.split(" ")(0), getStructType(filedName.split(" ")(1)), nullable = true)
      })
  }

  /**
    * 类型匹配
    */
  def getStructType(t: String): DataType = {
    //字段类型
    t match {
      case "int" => IntegerType
      case "String" => StringType
      case "string" => StringType
      case "double" => DoubleType
      case "timestamp" => TimestampType
    }
  }

  //根据df的header信息获取建立hive表的head
  //  [(id,IntegerType),(category,StringType)]  ==>id int,category string
  def getHeadContent(dfheads: Array[(String, String)]): String = {
    dfheads.map(head => head._1 + " " + hiveTypeMap(head._2)).mkString(",")
  }
}
