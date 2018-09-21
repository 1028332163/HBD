package com.esoft.dae

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by asus on 2017/3/16.
  */
object ContextContainer {
  val sc = new SparkContext(new SparkConf().setAppName("testCase").setMaster("local"))
}
