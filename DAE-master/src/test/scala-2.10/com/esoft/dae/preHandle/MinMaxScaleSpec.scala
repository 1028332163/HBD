package com.esoft.dae.preHandle

import com.esoft.dae.{TestUtil, ContextContainer}
import org.scalatest.{Tag, Matchers, FunSpec}

/**
  * Created by asus on 2017/3/16.
  */
class MinMaxScaleSpec extends FunSpec with Matchers {
  val executor = minMaxScale
  val sc = ContextContainer.sc

  val basePath = "testCase/MinMaxScale/"
  val taskId = "1"

  describe("check minMaxScale") {
    it("simple test case",Tag("runTest")) {

      val inputPath = "input.parquet"
      val colNamesPara = "col1"
      val max = "1" //映射区间的最大值
      val min = "0" //映射区间的最小值
      val saveOriCol = "true"
      val head = "col1 double,col2 double"
      val outputPath = "output.parquet"
      val args = Array(basePath, inputPath,colNamesPara, max, min, saveOriCol, head,outputPath, taskId)

      executor.exec(sc,args)
      val wishDf = "wishOut.parquet"
//      TestUtil.compareDf(basePath + outputPath, basePath + wishDf) should equal(true)
    }



  }
}
