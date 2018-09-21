package com.esoft.dae

import java.sql.Timestamp

import com.esoft.dae.util.handleUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by asus on 2017/3/15.
  */
object CsvToParquet {
  val sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local"))
  val sqlContext = new SQLContext(sc)

  val basePath = "testCase/MinMaxScale/"

  val inCsv = "input.csv"
  val inHead = "rownum int,"+
    "col1 double,col2 double,label double,labelstr string"
  val inParquet = "input.parquet"

  val wishOutCsv = "wishOut.csv"
  val wishOutHead = "rownum int,"+
    "col1 double,col2 double,label double,labelstr string,nor_col1 double"
  val wishOutParquet = "wishOut.parquet"

  def main(args: Array[String]) {
    csvToParquet(basePath+inCsv,basePath+inParquet,inHead)
    csvToParquet(basePath+wishOutCsv,basePath+wishOutParquet,wishOutHead)
  }

  def csvToParquet(inputPath:String,outputPath:String,head:String): Unit ={

    val input: RDD[String] = sc.textFile(inputPath)
    val rddWithOutHead: RDD[(String, Long)] =
      input.zipWithIndex.filter {
        //给数据集添加索引，并删除第一条表头和没数据的行
        line => line._2 != 0
      }
    val resultRDD: RDD[String] = rddWithOutHead.map {
      //将行号放到前面
      line =>
        line._2 + "," + line._1
    }

    //解析hive 表的表头
    val schema: StructType = handleUtil.getSchema(head)
    val headArr = head.split(",")
    //得到类型的RDD
    val typeArray: Array[String] = headArr.map { x => x.split(" ")(1) }
    //[int,int]
    //将RDD转换成ROW
    val dataInRow: RDD[Row] = resultRDD.map { row =>
      //csvData为从文件中读取的数据
      val csvData = row.split(",")
      val rowData: Array[String] =
        if (headArr.length == csvData.length)
          csvData
        else {
          Range(0, headArr.length - csvData.length).foldLeft(csvData)((arr, i) => arr :+ "")
        }
      //类型和数据对应
      //[1,a]=>[(1,int),(a,string)]
      val mapArray: Array[(String, String)] = rowData.zip(typeArray)
      Row(mapArray.map { x =>
        x._2 match {
          case "int" => try {
            //replace是为了处理  "1"  这种被引号括起来的情况
            x._1.toString.replace("\"", "").toInt
          } catch {
            case ex: Exception => null
          }
          case "string" =>
            try {
              if ("" == x._1.toString.trim)
                null
              else
                x._1.toString
            } catch {
              case ex: Exception => null
            }
          case "double" => try {
            x._1.toString.replace("\"", "").toDouble
          } catch {
            case ex: Exception => null
          }
          case "timestamp" => try {
            Timestamp.valueOf(x._1.toString)
          } catch {
            case ex: Exception => null
          }
        }
      }: _*) //_*表示将集合所有的内容构造对象
    }
    //将Schema应用于RDD
    val dataFrame = sqlContext.createDataFrame(dataInRow, schema)

    dataFrame.write.format("parquet")
      .mode(SaveMode.Overwrite)
      .save(outputPath)
  }
}
