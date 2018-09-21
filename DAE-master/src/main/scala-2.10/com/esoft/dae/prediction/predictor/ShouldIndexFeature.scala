package com.esoft.dae.prediction.predictor

import com.esoft.dae.util.handleUtil
import org.apache.spark.ml.feature.{VectorIndexerModel, StringIndexerModel, StringIndexer}
import org.apache.spark.sql.DataFrame

/**
  * Created by asus on 2017/3/23.
  */
trait ShouldIndexFeature extends BasePredictor {
  override def setDfToUse(): DataFrame = {
    //stringindex每一个String型的特征列;并合并成向量
    val strFeatures = handleUtil.getStrFeatures(dataFrame.dtypes, colNamesPara)
    var tmpDf = dataFrame
    val indexer = new StringIndexer()
    for (i <- strFeatures.indices) {
      val col = strFeatures(i)
      indexer.setInputCol(col)
        .setOutputCol(col + "_indexed")
      val model = StringIndexerModel.load(basePath + alPath + "/" + col)
      tmpDf = model.transform(tmpDf)
    }
    val combinedDf = handleUtil.combineCol(tmpDf, handleUtil.getClassfierFeatures(colNamesPara, dataFrame.dtypes))
    //index特征向量
    val vectorIndexer = VectorIndexerModel.load(basePath + alPath + "/vectorIndexer")
    vectorIndexer.transform(combinedDf)
  }
}
