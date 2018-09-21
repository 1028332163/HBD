package com.esoft.dae.prediction.predictor

import com.esoft.dae.util.handleUtil
import org.apache.spark.ml.feature.StringIndexerModel
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, DataFrame, SaveMode}

/**
  * Created by asus on 2017/4/11.
  */
trait HaveMulProbability extends BasePredictor {
  val labelIndexcer = StringIndexerModel.load(basePath + alPath + "/labelIndexer")

  override def saveShowDf(): Unit = {
    //普通数据表
    val showDfCols = if (addCols != "")
      "rownum" +: addCols.split(",") :+ predictionCol :+ "probability"
    else
      Array("rownum", predictionCol, "probability")
    val oriDf = fittedDf.select(showDfCols.head, showDfCols.tail: _*)
    val showRdd = oriDf.map { row =>
      val newRow =
        for (i <- 0 until row.size - 1) yield row(i)
      Row.fromSeq(newRow ++ row(row.size - 1).asInstanceOf[Vector].toArray)
    }
    val labels = StringIndexerModel.load(basePath + alPath + "/labelIndexer").labels
    val reg = "[a-zA-Z0-9]".r//正则表达式，只取string中的数字和字母
    val showSchema =
      StructType(oriDf.schema.dropRight(1) ++
        (for(i <- labels.indices)yield
         new StructField(reg.findAllIn(labels(i)).toList.mkString("").toLowerCase()+"_pro"+i, DoubleType, nullable = true))
      )
    val showDf = sqlContext.createDataFrame(showRdd,showSchema)
    val showDfPath = handleUtil.getDfShowPath(outputPath) + "1.parquet"
    viewDf(showDf,"showDf")
    showDf.write.format("parquet")
      .mode(SaveMode.Overwrite)
      .save(basePath + showDfPath)
    updateTaskInsData(taskId.toInt, showDfPath, showDf.dtypes, ONLY_ONE_TABLE, "no json")

  }

  override def setFinalDf(): DataFrame = {
    val finalCols = dataFrame.columns  :+ predictionCol
    val selectedDf = fittedDf.select(finalCols.head, finalCols.tail: _*)
    val selectedSchema = selectedDf.schema
    val finalSchema = StructType(selectedSchema.dropRight(1)
      :+ new StructField(predictionCol, StringType, nullable = true)
    )
    val labels = StringIndexerModel.load(basePath + alPath + "/labelIndexer").labels
    val finalRdd = selectedDf
      .map { row =>
        val newRow =
          for (i <- 0 until row.size) yield {
            i match {
              case j if i == row.size - 1 => labels(row(j).toString.toDouble.toInt)
              case _ => row(i)
            }
          }
        Row.fromSeq(newRow)
      }
    sqlContext.createDataFrame(finalRdd, finalSchema)
  }
}
