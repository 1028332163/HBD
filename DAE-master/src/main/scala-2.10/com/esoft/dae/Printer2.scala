package com.esoft.dae

import com.esoft.dae.util.LoggerUtil

/**
  * Created by asus on 2017/3/7.
  */
object Printer2 {
  def main(args: Array[String]) {
    val logger = LoggerUtil.getLogger("2")
    for (i <- Range(0, 15)) {
      logger.info("2")
      Thread.sleep(1000)
    }
  }
}
