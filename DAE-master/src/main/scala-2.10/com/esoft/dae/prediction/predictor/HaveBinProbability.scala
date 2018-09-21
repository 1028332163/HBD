package com.esoft.dae.prediction.predictor

import com.esoft.dae.util.handleUtil
import org.apache.spark.ml.feature.StringIndexerModel
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.types.{StringType, DoubleType, StructField, StructType}
import org.apache.spark.sql.{Row, DataFrame, SaveMode}

/**
  * Created by asus on 2017/3/23.
  */
trait HaveBinProbability extends BasePredictor {
  override def saveShowDf(): Unit = {
    val showDfCols = if (addCols != "")
      "rownum" +: addCols.split(",") :+ "probability" :+ predictionCol
    else
      Array("rownum", "probability", predictionCol)
    val showDf = finalDf.select(showDfCols.head, showDfCols.tail: _*)
    showDf.show()
    val showDfPath = handleUtil.getDfShowPath(outputPath) + "1.parquet"
    showDf.write.format("parquet")
      .mode(SaveMode.Overwrite)
      .save(basePath + showDfPath)
    updateTaskInsData(taskId.toInt, showDfPath, showDf.dtypes, ONLY_ONE_TABLE, "no json")
  }

  override def setFinalDf(): DataFrame = {

    val finalCols = dataFrame.columns :+ "probability" :+ predictionCol
    val selectedDf = fittedDf.select(finalCols.head, finalCols.tail: _*)
    val selectedSchema = selectedDf.schema
    val finalSchema = StructType(selectedSchema.dropRight(2)
      :+ new StructField("probability", DoubleType, nullable = true)
      :+ new StructField(predictionCol, StringType, nullable = true)
    )

    val labels = StringIndexerModel.load(basePath + alPath + "/labelIndexer").labels
    val finalRdd = selectedDf
      .map { row =>
        val newRow =
          for (i <- 0 until row.size) yield {
            i match {
              //probabili列，将向量拆分
              case j if i == row.size - 2 => row(i).asInstanceOf[Vector](1)
              case j if i == row.size - 1 => labels(row(j).toString.toDouble.toInt)
              case _ => row(i)
            }
          }
        Row.fromSeq(newRow)
      }
    sqlContext.createDataFrame(finalRdd, finalSchema)
  }
}
