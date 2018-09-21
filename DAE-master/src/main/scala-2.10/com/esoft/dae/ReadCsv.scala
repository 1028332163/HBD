package com.esoft.dae

import com.esoft.dae.util.LoggerUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

/**
  * Created by asus on 2017/3/7.
  */
object ReadCsv {
  def test(): String = {
    "0"
  }

  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local"))
    exec(sc, "data/kddcup.data.gz", ",", "true")
  }

  def exec(sc: SparkContext, filePath: String, delimiter: String, hasHeader: String): String = {
    val logger = LoggerUtil.getLogger("readCsv")
    logger.info("read file:" + filePath)
    val sqlContext = new SQLContext(sc)
    val dataFrame = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", hasHeader) // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .option("delimiter", delimiter)
      .option("charset", "UTF-8")
      .option("nullValue", "Na")
      .load(filePath)
    //    val dataFrame = sqlContext.csvFile(, , inferSchema = true, )
    val dTypes = dataFrame.dtypes.map(item => getType(item._2))
    val data: Array[Seq[Any]] = dataFrame.head(5).map(_.toSeq.map(_.toString))
    case class FinalVO(column_names: Array[String], column_types: Array[String], data: Array[Seq[Any]])
    val finalVO = FinalVO(dataFrame.columns, dTypes, data)
    implicit lazy val formats = Serialization.formats(NoTypeHints)
    val json = Serialization.write(finalVO)
    logger.info(filePath + ":" + json)
    json

  }

  def getType(dfType: String): String = {
    dfType match {
      case "TimestampType" => "timestamp"
      case "StringType" => "string"
      case "DoubleType" => "double"
      case "IntegerType" => "double"
      case _ => dfType
    }
  }


}
