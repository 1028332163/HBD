package com.esoft.dae.preHandle

import breeze.stats.distributions.Gaussian
import com.esoft.dae.dao.BaseDao
import com.esoft.dae.util.handleUtil
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SaveMode, Row, SQLContext}
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

/**
  *
  * @author liuzw
  *
  *         将太大的数字缩小至一个数字
  *         将太小的数字增大至一个数字
  *
  */
class outlierHandler(taskId: String) extends BaseDao(taskId) {
  def outlierHandle(sc: SparkContext, basePath: String, inputPath: String, ////////该方法的标准差需要确认
                    colNamesPara: String, handleMethod: String,
                    numMax: String, numMin: String,
                    perMax: String, perMin: String,
                    zMax: String, zMin: String,
                    outputPath: String, taskId: String) {

    val sqlContext = new SQLContext(sc)
    val dataFrame = sqlContext.read.parquet(basePath + inputPath)
    //    val dataFrame = csvUtil.readCsv(sc, sqlContext, basePath + inputPath)
    viewDf(dataFrame, "dataFrame")
    //////////////////////参数处理
    val colNames = colNamesPara.split(",")
    val dfCols = dataFrame.columns
    val handleColIndexs =
      for (col <- dfCols.indices if colNames.contains(dfCols(col))) yield col
    //将最小值转化为实际的数字,构造index->(min,max)的map
    var splitParaMap: Map[Int, (Double, Double)] = Map()

    for (i <- 0 until colNames.size) yield {
      val splitCol = colNames(i)
      val perfectDfCol = dataFrame.select(splitCol).na.drop //计算最大值最小值时要把缺失值扔掉
      val dfSize = perfectDfCol.count

      val (min, max) =
        if (dfSize == 0) {
          (0.0, 0.0) //该列全部为缺失值则该列所有的值将被替换为0
        } else if ("percent".equals(handleMethod)) {
          //按照百分数求得最大值最小值
          val thresholdDf = handleUtil.filteByIndex(perfectDfCol.orderBy(splitCol),
            Array((dfSize * perMin.toDouble).round, (dfSize * perMax.toDouble).round - 1))
          val threshold = handleUtil.getCol(thresholdDf)
          (threshold(0).asInstanceOf[Double], threshold(1).asInstanceOf[Double])
        } else if ("zscore".equals(handleMethod)) {
          //按照zscore来处理
          val colSta = perfectDfCol.describe(splitCol).map { row => row(1).toString.toDouble }.collect()
          val mu = colSta(1) //均值估计
          val sigma = colSta(2) * dfSize / dfSize - 1 //方差估计
          val gModel = new Gaussian(mu, sigma)
          (gModel.icdf(zMin.toDouble), gModel.icdf(zMax.toDouble))
        } else {
          //直接按照输入的数值进行处理
          (numMin.toDouble, numMax.toDouble)
        }
      splitParaMap += handleColIndexs(i) ->(min, max)
    }
    //异常值处理
    val finalRdd = dataFrame.map { row =>
      val newRow = for (i <- 0 until row.size) yield {
        if (handleColIndexs.contains(i)) {
          //需要进行处理的列
          val min = splitParaMap(i)._1
          val max = splitParaMap(i)._2
          if (row(i) == null) {
            min
          } else if (row(i).toString.toDouble < min)
            min
          else if (row(i).toString.toDouble > max)
            max
          else
            row(i)
        } else {
          //不需要处理的列
          row(i)
        }
      }
      Row.fromSeq(newRow)
    }
    val finalDf = sqlContext.createDataFrame(finalRdd, dataFrame.schema)
    finalDf.show()

    implicit lazy val formats = Serialization.formats(NoTypeHints)
    val json = Serialization.write(
      splitParaMap.map {
        one => Col(dfCols(one._1), one._2._1, one._2._2)
      }
    )
    println(json)
    //  写入数据
    finalDf.write.format("parquet").mode(SaveMode.Overwrite).save(basePath + outputPath)
    super.flagSparked(taskId.toInt, outputPath, handleUtil.getHeadContent(finalDf.dtypes), json)

  }

  case class Col(colName: String, min: Double, max: Double)

}

object outlierHandler {

  def main(args: Array[String]) {
    val basePath = ""
    val inputPath = "data/tree.csv"

    val colNames = "col1,col2"
    val handleMethod = "percent" //num（double型）,percent(0-1的double型),zscore(0-1的double型)
    val max = "0.7" //大于多少算是异常值
    val min = "0.2" //小于多少算是异常值
    val head = "col1 double,col2 double"

    val outputPath = "out1/lzw_typeConvert_Out.parquet"
    val taskID = 815
    //  val args = Array(basePath,inputPath,colNames,handleMethod,max,min,head,outputPath,taskID.toInt)
    exec(handleUtil.getContext("outlierHandler"), args)

  }

  def exec(sc: SparkContext, args: Array[String]): Unit = {
    implicit lazy val formats = org.json4s.jackson.Serialization.formats(org.json4s.NoTypeHints)
    case class ArgVO(inputPath: String, ////////该方法的标准差需要确认
                     featuresCol: String, handleMethod: String,
                     numMax: String, numMin: String,
                     perMax: String, perMin: String,
                     zMax: String, zMin: String, //head需要处理
                     outputPath: String)

    val executer = new outlierHandler(args.last)
    try {
      args.foreach { x => executer.logger.info("inArg-" + x) }
      val basePath = args(0)
      val ArgVO(inputPath: String, ////////该方法的标准差需要确认
      colNamesPara: String, handleMethod: String, numMax: String, numMin: String,
      perMax: String, perMin: String,
      zMax: String, zMin: String, //head需要处理
      outputPath: String)
      = org.json4s.jackson.Serialization.read[ArgVO](args(1))
      val taskId = args(2)
      //executer.checkArgsNum(9, args.length)
      executer.outlierHandle(sc: SparkContext, basePath: String, inputPath: String, ////////该方法的标准差需要确认
        colNamesPara: String, handleMethod: String, numMax: String, numMin: String,
        perMax: String, perMin: String,
        zMax: String, zMin: String, //head需要处理
        outputPath: String, taskId: String)
    } catch {
      case ex: Throwable => executer.handleException(args(args.length - 1).toInt, ex, "lineREval")
    }
  }

}
