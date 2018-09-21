package com.esoft.dae

import org.scalatest.{FunSpec, Matchers}

/**
  * Created by asus on 2017/3/14.
  */
class ReadCsvSpec extends FunSpec with Matchers{
  describe("SparkJobUtils.configToSparkConf") {
    it("should be 1") {
      val result = ReadCsv
      ReadCsv.test should equal ("0")
    }

  }
}
