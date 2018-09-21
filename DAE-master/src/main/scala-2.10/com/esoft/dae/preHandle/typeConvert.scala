package com.esoft.dae.preHandle

import com.esoft.dae.dao.BaseDao
import com.esoft.dae.evaluate.mllib.mutiClassEval._
import com.esoft.dae.util.{handleUtil, csvUtil}
import org.apache.spark.sql.{SaveMode, Row, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
  * @author Liuzw
  *         字段类型进行转换
  */
class typeConvert(taskId: String) extends BaseDao(taskId) {
  def typeConvert(sc: SparkContext, basePath: String, inputPath: String,
                  convertPara: String,
                  outputPath: String, taskId: String) {

    val sqlContext = new SQLContext(sc)
    val dataFrame = sqlContext.read.parquet(basePath + inputPath)
    //        val dataFrame = csvUtil.readCsv(sc, sqlContext, basePath + inputPath)
    viewDf(dataFrame, "dataFrame")
    ////////////////////传入参数处理成可以使用的形式
    assert(convertPara != "", "没有选择进行转换的列")
    val dfCols: Array[String] = dataFrame.columns
    //将convertPara  从“col1:double,
    // col2:string,”转为"角标->类型" 的map

    val conParaArray: Array[Array[Any]] = convertPara.split(",").map { onePara =>
      Array(dfCols.indexOf(onePara.split(" ")(0)), onePara.split(" ")(1))
    }
    var conParaMap: Map[Int, String] = Map()
    for (i <- conParaArray.indices) {
      //如果相同的列既转double又转Stirng 会以后面的为准
      conParaMap += conParaArray(i)(0).toString.toInt -> conParaArray(i)(1).toString
    }
    println(conParaMap)
    //    修改head：原来的head是原数据的表头，由于数据的类型发生变化，所以需要修改来变成新的数据类型的表头
    //    owNum double,U1 double,U2 string,U3 string,rate int,frequency int,windDirection double
    val head: String = handleUtil.getHeadContent(dataFrame.dtypes).split(",").map { oneCol =>
      val oneColArr: Array[String] = oneCol.split(" ")
      val colIndex = dfCols.indexOf(oneColArr(0))
      if (conParaMap.keySet.contains(colIndex)) {
        //发生修改的列
        "" + oneColArr(0) + " " + conParaMap(colIndex)
      } else {
        //没有发生修改的列
        "" + oneColArr(0) + " " + oneColArr(1)
      }
    }.mkString(",")
    //    schema

    //处理数据类型
    //[0,double],[1,string]
    val finalRDD = dataFrame.map {
      row =>
        val newRow = for (i <- 0 until row.size) yield {
          if (conParaMap.contains(i)) {
            //需要变化的列
            println(i)
            println(conParaMap(i))
            conParaMap(i) match {
              case "int" => try {
                row(i).toString.toDouble.toInt
              } catch {
                case ex: Exception => null
              }
              case "string" => try {
                row(i).toString
              } catch {
                case ex: Exception => null
              }
              case "String" => try {
                row(i).toString
              } catch {
                case ex: Exception => null
              }
              case "double" => try {
                row(i).toString.toDouble
              } catch {
                case ex: Exception => null
              }
            }
          } else {
            //不需要变化的列
            row(i)
          }
        }
        Row.fromSeq(newRow)
    }
    val finalDF = sqlContext.createDataFrame(finalRDD, handleUtil.getSchema(head))
    finalDF.show()
    finalDF.printSchema()
    //    写入数据
    finalDF.write.format("parquet").mode(SaveMode.Overwrite).save(basePath + outputPath)
    super.flagSparked(taskId.toInt, outputPath, head, "no json")
  }

}

object typeConvert {

  def main(args: Array[String]) {
    val basePath = ""
    val inputPath = "data/tree.csv"

    val convertPara = "col1:double,col2:int,col3:string"

    val outputPath = "out/lzw_typeConvert_Out.parquet"
    val taskID = "815"

    //    val args = Array(basePath, inputPath, convertPara, head, outputPath, taskID)
    exec(handleUtil.getContext("typeConvert"), args)

  }

  def exec(sc: SparkContext, args: Array[String]): Unit = {
    implicit lazy val formats = org.json4s.jackson.Serialization.formats(org.json4s.NoTypeHints)
    case class ArgVO(inputPath: String,
                  convertPara: String,
                  outputPath: String)

    val executer = new typeConvert(args.last)
    try {
      args.foreach(println(_))
      val basePath = args(0)
      val ArgVO(inputPath: String,
                  convertPara: String,
                  outputPath: String)
      = org.json4s.jackson.Serialization.read[ArgVO](args(1))
      val taskId = args(2)
      //executer.checkArgsNum(6, args.length)
      executer.typeConvert(sc: SparkContext, basePath: String, inputPath: String,
                  convertPara: String,
                  outputPath: String, taskId: String)
    } catch {
      case ex: Throwable => executer.handleException(args(args.length - 1).toInt, ex, "typeConvert")
    }
  }

}
