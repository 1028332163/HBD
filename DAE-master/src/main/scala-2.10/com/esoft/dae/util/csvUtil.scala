package com.esoft.dae.util

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, DataFrame, SQLContext}

/**
  * Created by asus on 2016/12/17.
  */
object csvUtil {

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
    }
  }

  def readCsv(sc: SparkContext, sqlContext: SQLContext, file: String): DataFrame = {
    ///////////////////读取本地的csv文件到rdd——构造表头，将rdd一行一行的插入
    val csvData = sc.textFile(file)
    val data2D = csvData.map { line => line.split(",") } //rdd[String]->rdd[array[String]]
    val columnNames = data2D.first(); //取出第一行作为表头
    columnNames.foreach { x => println(x) }
    //array[String]->StructType
    val schema = StructType(columnNames.map(fieldName => StructField(fieldName, DoubleType, nullable = true)))
    //得到Rdd中的数据部分
    val rows = data2D.filter { row => row(0) != columnNames(0) }
    val rows1 = rows.map(row => row.map { item => {
      item.toDouble
    }
    }); //转为double型

    val rows2 = rows1.map { row => Row.fromSeq(row) } //rdd[Array[vector]]->rdd[Row]
    //创建dataFrame
    val dataFrame = sqlContext.createDataFrame(rows2, schema)
    dataFrame
  }

  def readIntCsv(sc: SparkContext, sqlContext: SQLContext, file: String): DataFrame = {
    ///////////////////读取本地的csv文件到rdd——构造表头，将rdd一行一行的插入
    val csvData = sc.textFile(file)
    val data2D = csvData.map { line => line.split(",") } //rdd[String]->rdd[array[String]]
    val columnNames = data2D.first(); //取出第一行作为表头
    columnNames.foreach { x => println(x) }
    //array[String]->StructType
    val schema = StructType(columnNames.map(fieldName => StructField(fieldName, IntegerType, nullable = true)))
    //得到Rdd中的数据部分
    val rows = data2D.filter { row => row(0) != columnNames(0) }
    val rows1 = rows.map(row => row.map { item => {
      item.toInt
    }
    })

    val rows2 = rows1.map { row => Row.fromSeq(row) } //rdd[Array[vector]]->rdd[Row]
    //创建dataFrame
    val dataFrame = sqlContext.createDataFrame(rows2, schema)
    dataFrame
  }

  def readStrCsv(sc: SparkContext, sqlContext: SQLContext, file: String): DataFrame = {
    ///////////////////读取本地的csv文件到rdd——构造表头，将rdd一行一行的插入
    val csvData = sc.textFile(file)
    val data2D = csvData.map { line => line.split(",") } //rdd[String]->rdd[array[String]]
    val columnNames = data2D.first(); //取出第一行作为表头
    columnNames.foreach { x => println(x) }
    //array[String]->StructType
    val schema = StructType(columnNames.map(fieldName => StructField(fieldName, StringType, nullable = true)))
    //得到Rdd中的数据部分
    val rows = data2D.filter { row => row(0) != columnNames(0) }
    val rows1 = rows.map(row => row.map { item => {
      item
    }
    }) //转为double型

    val rows2 = rows1.map { row => Row.fromSeq(row) } //rdd[Array[vector]]->rdd[Row]
    //创建dataFrame
    val dataFrame = sqlContext.createDataFrame(rows2, schema)
    dataFrame
  }

  def readCobCsv(sc: SparkContext, sqlContext: SQLContext, file: String, head: String, sep: String = ","): DataFrame = {
    val input = sc.textFile(file)

    val rddWithOutHead = input.zipWithIndex.filter {
      //给数据集添加索引，并删除第一条
      line =>
        line._2 != 0
    }

    val resultRDD: RDD[String] = rddWithOutHead.map {
      //将行号放到前面
      line =>
        line._2 + sep + line._1
    }
    //解析hive 表的表头
    val schema = handleUtil.getSchema(head)
    //得到类型的RDD
    val typeArray = head.split(",").map { x => x.split(" ")(1) }
    //将RDD转换成ROW
    val dataInRow = resultRDD.map(_.split(sep)).map { p =>
      //类型和数据对应
      val mapArray: Array[(String, String)] = p.zip(typeArray)
      Row(mapArray.map { x =>
        x._2 match {
          case "int" => try {
            x._1.toString.toInt
          } catch {
            case ex: Exception => null
          }
          case "string" => x._1.toString
          case "double" => try {
            x._1.toString.toDouble
          } catch {
            case ex: Exception => null
          }
        }
      }: _*) //_*表示将集合所有的内容构造对象
    }
    //将Schema应用于RDD
    sqlContext.createDataFrame(dataInRow, schema)
  }

}
