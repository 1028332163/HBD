package com.esoft.dae.dataSource

import java.sql.Timestamp

import com.esoft.dae.util.handleUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.SparkContext
import com.esoft.dae.dao.BaseDao
import org.apache.spark.sql.SaveMode
import org.json4s.jackson.Serialization

/**
  * @author taoshi
  *         接入数据源，生成parquet 文件
  */
class DataSourceParquet(taskId: String) extends BaseDao(taskId) {
  def dataSource(sc: SparkContext, basePath: String, inputPath:String,
                 headPara: String, maxNullPro: String, sep: String, whetherHeader: String,
                 outputPath: String, taskId: String): Unit = {
    logger.warn("++++++++++++++++++++++++++++++++++")
    logger.warn("+++uniFlag:0555")
    logger.warn("++++++++++++++++++++++++++++++++++")
    val head = "rownum int," + headPara
    val sqlContext: SQLContext = new SQLContext(sc)
    val oriDataFrame =
      if (inputPath.endsWith(".parquet")) {
        logger.info("read parquet")
        sqlContext.read.parquet(basePath + inputPath)
      } else {
        logger.info("read file")
        val input: RDD[String] = sc.textFile(basePath + inputPath)
        logger.info("first line:" + input.first())
        import sqlContext.implicits._
           viewDf(input.toDF(), "inputDf")

        val rddWithOutHead: RDD[(String, Long)] =
          if (whetherHeader == "1") {
            input.zipWithIndex.filter {
              //给数据集添加索引，并删除第一条表头和没数据的行
              line => line._2 != 0
            }
          } else input.zipWithIndex

        val resultRDD: RDD[String] = rddWithOutHead.map {
          //将行号放到前面
          line =>
            line._2 + sep + line._1
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
          val csvData = row.split(sep)
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
            //            (a,int)
            x._2 match {
              case "int" => try {
                //replace是为了处理  "1"  这种情况
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
        sqlContext.createDataFrame(dataInRow, schema)
      }
    viewDf(oriDataFrame, "oriDf")
    val dfWidth = oriDataFrame.columns.length - 1 //减1是为了去除rowNum
    val minNonNull = 1 + (dfWidth - dfWidth * maxNullPro.toDouble).toInt //加1是为了消除rowNum的影响
    logger.info("minNonNull:" + minNonNull)
    val dataFrame = oriDataFrame.na.drop(minNonNull)
    viewDf(dataFrame, "resultDf")
    //更新数据库状态
    dataFrame.write.format("parquet")
      .mode(SaveMode.Overwrite)
      .save(basePath + outputPath)
    super.flagSparked(taskId.toInt, outputPath, head, "no json")
  }

  override def getSpeHint(ex: Throwable): String = {
    val exString = ex.toString
    exString match {
      case sepErr if exString.contains("java.lang.ArrayIndexOutOfBoundsException")
      => "数据集分隔符错误"
      case _ => ""
    }
  }
}

object DataSourceParquet {
  def main(args: Array[String]): Unit = {
    //        val args = Array(basePath: String, inputPath: String, head: String, minNonNullPro: String, sep: String, whetherHeader: String, outputPath: String, taskId: String)
    //
    //
    //        val args = csvUtil.handleArg()

    exec(handleUtil.getContext("dataSource"), args)

    //    exec(handleUtil.getContext("dataSource"), args)
  }

  def exec(sc: SparkContext, args: Array[String]): Unit = {
    implicit lazy val formats = Serialization.formats(org.json4s.NoTypeHints)
    case class ArgVO(inputPath: String,
                     head: String, maxNullPro: String, sep: String, whetherHeader: String,
                     outputPath: String)
    val executer = new DataSourceParquet(args.last)

    try {
      args.foreach { x => executer.logger.info("inArg-" + x) }
      val basePath = args(0)
      val ArgVO(inputPath, head, maxNullPro, sep, whetherHeader, outputPath)
      = org.json4s.jackson.Serialization.read[ArgVO](args(1))
      val taskId = args(2)
      ////executer.checkArgsNum(8, args.length)
      executer.dataSource(sc: SparkContext, basePath: String, inputPath: String,
        head: String, maxNullPro: String, sep: String, whetherHeader: String,
        outputPath: String, taskId: String)
    } catch {
      case ex: Throwable => executer.handleException(args(args.length - 1).toInt, ex, "dataSourceParquet")
    }

  }

}
