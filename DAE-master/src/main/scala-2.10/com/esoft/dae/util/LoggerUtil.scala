package com.esoft.dae.util

import org.apache.log4j.Logger
import org.apache.log4j.PatternLayout
import org.apache.log4j.RollingFileAppender

/**
  * Created by asus on 2017/2/24.
  */
object LoggerUtil {
  def getLogger(taskId: String): Logger = {
    val preLogPath = handleUtil.loadProperties("log.dir")
    val logger = Logger.getLogger("L" + taskId)
    // 設定是否繼承父Logger。默認為true。繼承root輸出.設定false後將不輸出root。
    logger.setAdditivity(true)
    // 生成新的Appender
    val appender = new RollingFileAppender()
    val conversionPattern = handleUtil.loadProperties("log.conversionPattern")
    val layout = new PatternLayout()
    layout.setConversionPattern(conversionPattern)
    appender.setLayout(layout)
    appender.setFile(preLogPath + taskId + ".log")
    appender.setAppend(true)

    // 适用当前配置
    appender.activateOptions()
    // 将新的Appender加到Logger中
    logger.addAppender(appender)
    logger
  }
}
