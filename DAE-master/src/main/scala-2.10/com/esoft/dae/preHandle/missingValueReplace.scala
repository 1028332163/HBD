package com.esoft.dae.preHandle

import com.esoft.dae.dao.BaseDao
import org.apache.spark.sql.functions
import org.apache.spark.SparkContext
import com.esoft.dae.util.handleUtil
import org.apache.spark.sql.{SaveMode, DataFrame, SQLContext}

/**
  * @author
  */
class missingValueReplace(taskId: String) extends BaseDao(taskId) {


  def execMissingValueHandle(sc: SparkContext, basePath: String, inputPath: String,
                             exeDrop: String, dropCols: String, maxNullPro: String,
                             exeReplace: String,
                             strCols: String, strBlank: String, strUD: String, strNew: String,
                             numCols: String, numNaN: String, numUD: String, numNew: String,
                             outputPath: String, taskId: String): Unit = {
    ///////////构建yarn-spark,从parquet中读取文件
    val sqlContext = new SQLContext(sc)
    val dataFrame = sqlContext.read.parquet(basePath + inputPath)
    //    val dataFrame = csvUtil.readCobCsv(sc, sqlContext, "data/test.csv", "rowNUm int,col1 int,col2 int,label string")
    viewDf(dataFrame, "dataFrame")
    //////////////////参数处理
    val dropedDf =
      if (exeDrop.toBoolean && "" != dropCols)
        dropNa(dataFrame, dropCols, maxNullPro)
      else
        dataFrame
    val finalDf =
      if (exeReplace.toBoolean)
        replaceNa(dropedDf, strCols, strBlank, strUD, strNew,
          numCols, numNaN, numUD, numNew)
      else
        dropedDf

    viewDf(finalDf, "finalDf")
    ////////////////////保存结果及保存json
    finalDf.write.format("parquet")
      .mode(SaveMode.Overwrite)
      .save(basePath + outputPath)
    super.flagSparked(taskId.toInt, outputPath, handleUtil.getHeadContent(finalDf.dtypes), "no json")
  }

  def dropNa(oriDf: DataFrame, dropCols: String, maxNullPro: String): DataFrame = {
    val dropColArr = dropCols.split(",")
    val colNum = dropColArr.length
    val minNotNullNum = colNum - (colNum * maxNullPro.toDouble).round
    oriDf.na.drop(minNotNullNum.toInt, dropColArr)
  }

  def replaceNa(oriDf: DataFrame,
                strCols: String, strBlank: String, strUD: String, strNew: String,
                numCols: String, numNaN: String, numUD: String, numNew: String): DataFrame = {
    val strReplacedDf =
      if ("" == strCols) oriDf
      else replaceStrNa(oriDf, strCols, strBlank, strUD, strNew)
    if ("" == numCols) strReplacedDf
    else replaceNumNa(strReplacedDf, numCols, numNaN, numUD, numNew)
  }

  def replaceStrNa(oriDf: DataFrame,
                   strCols: String, strBlank: String, strUD: String, strNew: String): DataFrame = {
    val strColArr = strCols.split(",")

    val nullHandledDf =
      if (strBlank.toBoolean)
        oriDf.na.fill(strNew, strColArr) //替换null值
      else
        oriDf

    var replacement = Map[String, String]()
    if (strBlank.toBoolean)
      replacement += ("" -> strNew)
    if ("" != strUD)
      replacement += (strUD -> strNew)

    if (replacement.nonEmpty)
      nullHandledDf.na.replace(strColArr, replacement)
    else nullHandledDf

  }

  def replaceNumNa(oriDf: DataFrame,
                   numCols: String, numNaN: String, numUD: String, numNew: String): DataFrame = {
    val numColArr = numCols.split(",")
    val handleCols =
      (df: DataFrame, newValue: String, cols: Array[String]) => {
        try {
          val nullHandledDf =
            if (numNaN.toBoolean)
              df.na.fill(newValue.toDouble, cols)
            else
              df
          viewDf(nullHandledDf, "nullHandledDf")
          if ("" == numUD)
            nullHandledDf
          else
            nullHandledDf.na.replace(cols, Map(numUD.toDouble -> newValue.toDouble))
        } catch {
          case ex: Exception => throw new Exception("数字列的被替换值或新替换值不是数字")
        }

      }
    numNew match {
      case "Min" =>
        numColArr.foldLeft(oriDf) {
          (df, col) =>
            val perfectDf =
              if ("" == numUD) //只扔掉缺失值
                df.na.drop(Array(col))
              else //扔掉缺失值和自定义缺失值
                df.na.drop(Array(col)).where(col + "!=" + numUD)
            val min = df.na.drop(Array(col)).select(functions.min(col)).first.get(0).toString
            handleCols(df, min, Array(col))
        }
      case "Max" =>
        numColArr.foldLeft(oriDf) {
          (df, col) =>
            val perfectDf =
              if ("" == numUD) //只扔掉缺失值
                df.na.drop(Array(col))
              else //扔掉缺失值和自定义缺失值
                df.na.drop(Array(col)).where(col + "!=" + numUD)
            val max = df.na.drop(Array(col)).select(functions.max(col)).first.get(0).toString
            handleCols(df, max, Array(col))
        }
      case "Mean" =>
        numColArr.foldLeft(oriDf) {
          (df, col) =>
            val perfectDf =
              if ("" == numUD) //只扔掉缺失值
                df.na.drop(Array(col))
              else //扔掉缺失值和自定义缺失值
                df.na.drop(Array(col)).where(col + "!=" + numUD)
            val mean = perfectDf.select(functions.mean(col)).first.get(0).toString
            handleCols(df, mean, Array(col))
        }
      case _ =>
        handleCols(oriDf, numNew, numColArr)
    }
  }

}

object missingValueReplace {

  def main(args: Array[String]): Unit = {
    exec(handleUtil.getContext("missingValueReplace"), args)
  }


  def exec(sc: SparkContext, args: Array[String]): Unit = {
    implicit lazy val formats = org.json4s.jackson.Serialization.formats(org.json4s.NoTypeHints)
    case class ArgVO(inputPath: String,
                     exeDrop: String, featuresCol: String, maxNullPro: String,
                     exeReplace: String,
                     featuresColString: String, strBlank: String, strUD: String, strNew: String,
                     featuresColDouble: String, numNaN: String, numUD: String, numNew: String,
                     outputPath: String)

    val executer = new missingValueReplace(args.last)
    try {
      args.foreach { x => executer.logger.info("inArg-" + x) }
      val basePath = args(0)
      val ArgVO(inputPath: String,
      exeDrop: String, dropCols: String, maxNullPro: String,
      exeReplace: String,
      strCols: String, strBlank: String, strUD: String, strNew: String,
      numCols: String, numNaN: String, numUD: String, numNew: String,
      outputPath: String)
      = org.json4s.jackson.Serialization.read[ArgVO](args(1))
      val taskId = args(2)
      //executer.checkArgsNum(5, args.length)
      executer.execMissingValueHandle(sc: SparkContext, basePath: String, inputPath: String,
        exeDrop: String, dropCols: String, maxNullPro: String,
        exeReplace: String,
        strCols: String, strBlank: String, strUD: String, strNew: String,
        numCols: String, numNaN: String, numUD: String, numNew: String,
        outputPath: String, taskId: String)
    } catch {
      case ex: Throwable => executer.handleException(args(args.length - 1).toInt, ex, "lineREval")
    }
  }

}
