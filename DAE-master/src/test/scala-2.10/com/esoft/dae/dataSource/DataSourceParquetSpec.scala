package com.esoft.dae.dataSource

import com.esoft.dae.{ContextContainer, TestUtil}
import org.scalatest.{Tag, Matchers, FunSpec}

/**
  * Created by asus on 2017/3/15.
  */
class DataSourceParquetSpec extends FunSpec with Matchers {

  val executor = DataSourceParquet
  val sc = ContextContainer.sc

  val basePath = "testCase/DataSourceParquet/"
  val taskId = "1"

  describe("check DataSourceParquet") {
    it("csv file should add rownum col",Tag("construction")) {

      val inputPath = "input.csv"
      val head = "rownum int,col1 double,col2 double,label double,labelstr string"
      val minNonNullPro = "0.5"
      val sep = ","
      val whetherHeader = "1"
      val outputPath = "output.parquet"

      val args = Array(basePath, inputPath, head, minNonNullPro, sep, whetherHeader, outputPath, taskId)

      executor.exec(sc,args)
      val wishDf = "wishOut.parquet"
      TestUtil.compareDf(basePath + outputPath, basePath + wishDf) should equal(true)
    }

  }
}
