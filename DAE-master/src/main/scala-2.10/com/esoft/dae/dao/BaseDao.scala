package com.esoft.dae.dao

import java.sql.{ResultSet, Connection, DriverManager}
import com.esoft.dae.util.handleUtil
import com.esoft.dae.util.LoggerUtil
import org.apache.spark.sql.{Row, DataFrame}


/**
  * @author liy
  */
class BaseDao(val taskId: String) {
  val logger = LoggerUtil.getLogger(taskId)
  val ORI_TABLE = "getData"
  val TWO_TABLE_LEFT = "getDataLeft"
  val TWO_TABLE_RIGHT = "getDataRight"
  val ONLY_ONE_TABLE = "getDataSum"
  val CHART_JSON = "getAssessmentReport"
  val MODEL_JSON = "getModelReport"
  //路径的名称前缀和算法的包名的map
  val modelPacMap = Map("ClusteringKMeans" -> "org.apache.spark.ml.clustering.KMeansModel",
    "Regressionlinear" -> "org.apache.spark.ml.regression.LinearRegressionModel",
    "LogisticRegression" -> "org.apache.spark.ml.classification.LogisticRegressionModel",
    "NaiveBayes" -> "org.apache.spark.ml.classification.NaiveBayesModel",
    "RegressionOrdinal" -> "org.apache.spark.ml.regression.IsotonicRegressionModel"
  )
  //根据算法的路径获得算法生成列的类型
  val preTypeMap = Map("kmeans" -> "int", "lineRegression" -> "double")
  //根据dateFrame的dtype的类型获得建立hive表的头部类型


  /**
    * 更新 新表 taskInstanceData 的状态
    */
  def updateTaskInsData(tarskID: Int, showDfSavePath: String, dfheads: Array[(String, String)], menuType: String, resultJson: String): Unit = {
    val conn = conncetion()

    try {
      val header = handleUtil.getHeadContent(dfheads)

      val prep = conn.prepareStatement("INSERT INTO TB_DAE_TARSK_INSTANCE_DATA" +
        "(HIVE_STATUS,TASK_ID,SPARK_RESULT,TITLE_CONTENT,DATATABLE_COLUMNS_LONGTEXT,THEAD_LONGTEXT,MENU_TYPE,SUMMARY_JSON_CONTENT) " +
        "VALUES(?,?,?,?,?,?,?,?)")
      prep.setString(1, "sparked")
      prep.setInt(2, tarskID)
      prep.setString(3, showDfSavePath)
      prep.setString(4, getHiveHead(header))
      prep.setString(5, getColumns(header))
      prep.setString(6, getTableThead(header))
      prep.setString(7, menuType)
      prep.setString(8, resultJson)

      logger.info(prep.toString)

      val result = prep.executeUpdate()
      logger.info("db update result:" + result)
    }
    finally {
      conn.close()
    }
  }

  def getHiveHead(header: String): String = {
    var hiveHead = ""
    val dataHeaderArray = header.split(",")
    for (i <- 0 until dataHeaderArray.length) {
      //字段 + 类型 此处给字段增加`
      val arrTemp = dataHeaderArray(i).split(" ")
      hiveHead += "`" + arrTemp(0) + "` " + arrTemp(1) + ","
    }
    hiveHead.substring(0, hiveHead.length - 1)
  }

  def getTableThead(header: String): String = {
    var tableThead = "<thead> <tr>"
    val dataHeaderArray = header.split(",")
    for (i <- 0 until dataHeaderArray.length) {
      //字段 + 类型 此处给字段增加`
      val arrTemp = dataHeaderArray(i).split(" ")
      tableThead = tableThead + "<th>" + arrTemp(0) + "</th>"
    }
    tableThead + "</tr></thead><tbody id='dataSetList'></tbody>"
  }

  def getColumns(header: String): String = {
    var columns = "["
    val dataHeaderArray = header.split(",")
    for (i <- 0 until dataHeaderArray.length) {
      //字段 + 类型 此处给字段增加`
      val arrTemp = dataHeaderArray(i).split(" ")
      columns = columns + "{'mDataProp' : '" + arrTemp(0).toLowerCase + "'},"
    }
    columns + " ]"
  }

  def conncetion(): Connection = {
    val url = handleUtil.loadProperties("jdbc.url")
    Class.forName(handleUtil.loadProperties("jdbc.driverClass"))
    val conn = DriverManager.getConnection(url)
    conn
  }

  /**
    * 更新 新表 taskInstanceData 的状态
    */
  def updateTaskInsData(tarskID: Int, showDfSavePath: String, dfheads: Array[(String, String)], tableThead: String, menuType: String, resultJson: String): Unit = {
    val conn = conncetion()

    try {
      val header = handleUtil.getHeadContent(dfheads)

      val prep = conn.prepareStatement("INSERT INTO TB_DAE_TARSK_INSTANCE_DATA" +
        "(HIVE_STATUS,TASK_ID,SPARK_RESULT,TITLE_CONTENT,DATATABLE_COLUMNS_LONGTEXT,THEAD_LONGTEXT,MENU_TYPE,SUMMARY_JSON_CONTENT) " +
        "VALUES(?,?,?,?,?,?,?,?)")
      prep.setString(1, "sparked")
      prep.setInt(2, tarskID)
      prep.setString(3, showDfSavePath)
      prep.setString(4, getHiveHead(header))
      prep.setString(5, getColumns(header))
      prep.setString(6, getTableThead(tableThead))
      prep.setString(7, menuType)
      prep.setString(8, resultJson)

      logger.info(prep.toString)

      val result = prep.executeUpdate()
      logger.info("db update result:" + result)
    }
    finally {
      conn.close()
    }
  }

  //通过SQL 语句查询单个值
  def query(sql: String): String = {
    var result = ""
    val conn = conncetion()

    try {
      // Configure to be Read Only
      val stat = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)

      // Execute Query
      val rs = stat.executeQuery(sql)

      // Iterate Over ResultSet
      if (rs.next) {
        result = rs.getString(1)
      }
      result
    }
    finally {
      conn.close()
    }

  }

  /**
    * 更新 旧表 taskInstance 的状态
    */
  def flagSparked(taskId: Int, outputPath: String, header: String, resultJson: String): Unit = {
    val conn = conncetion()
    try {
      val tableName = "p_" + taskId
      val prep = conn.prepareStatement("UPDATE TB_DAE_TARSK_INSTANCE SET  INS_STATUS = ?,  TITILE_CONTENT = ?,DATATABLE_COLUNMS_LONGTEXT = ?,THEAD_LONGTEXT = ?,SUMMARY_JSON_CONTENT = ?,SPARK_RESULT = ?,TABLE_NAME = ? WHERE ID = ?")
      prep.setString(1, "sparked")
      prep.setString(2, getHiveHead(header))
      prep.setString(3, getColumns(header))
      prep.setString(4, getTableThead(header))
      prep.setString(5, resultJson)
      prep.setString(6, "/"+outputPath)
      prep.setString(7, tableName)
      prep.setInt(8, taskId)

      logger.info(prep.toString)

      val result = prep.executeUpdate()
      logger.info("db update result:" + result)
    }
    finally {
      conn.close()
    }
  }

  def flagFail(tarskID: Int): Unit = {
    val conn = conncetion()
    try {
      val prep = conn.prepareStatement("UPDATE TB_DAE_TARSK_INSTANCE SET  INS_STATUS = ? WHERE ID = ?")
      prep.setString(1, "sparkfal")
      prep.setInt(2, tarskID)
      logger.info(prep.toString)
      val result = prep.executeUpdate()
      logger.info("db update result:" + result)
    }
    finally {
      conn.close()
    }
  }

  //根据路径获得算法的名称
  def getModel(path: String): String = {
    modelPacMap(path.split("/")(1))
  }

  //根据路径获得算法的名称
  def getPreType(path: String): String = {
    preTypeMap(path.split("/")(0))
  }

  def handleException(taskId: Int, ex: Throwable, model: String): Unit = {
    //    logger.info(ex.getStackTraceString) //全部信息
    //    ex.printStackTrace()
    //    logger.info(ex.getMessage)信息
    //    logger.info(ex.toString)异常类型 + 信息
    val hint = getHint(ex)

    logger.error(ex.getStackTraceString)
    logger.error("interpreter:++++++++++++++")
    logger.error(hint)
    flagHint(taskId, hint)
  }

  def flagHint(tarskID: Int, hint: String): Unit = {
    val conn = conncetion()
    try {
      val prep = conn.prepareStatement("UPDATE TB_DAE_TARSK_INSTANCE SET  INS_STATUS = ?,ERROR_HINT = ? WHERE ID = ?")
      prep.setString(1, "sparkfal")
      prep.setString(2, hint)
      prep.setInt(3, tarskID)
      logger.info(prep.toString)

      val result = prep.executeUpdate()
      logger.info("db update result:" + result)
    }
    finally {
      conn.close()
    }
  }

  //根据异常翻译向用户返回的错误信息
  def getHint(ex: Throwable): String = {
    val exString = ex.toString
    (exString match {
      case filePathError if exString.contains("Input path does not exist")
      => "文件路径不存在:"
      case netError if exString.contains("java.net.ConnectException")
      => "网络连接错误:"
      case schemaError if exString.contains("org.apache.spark.sql.catalyst." +
        "CatalystTypeConverters$StructConverter.toCatalystImpl")
      => "创建数据集的实际列数大于表头的列数:"
      case duplicateCol if exString.contains("org.apache.spark.sql.AnalysisException: Duplicate column(s)")
      => "数据集的列名重复:"
      case _ => getSpeHint(ex)
    }) + exString
  }

  def getSpeHint(ex: Throwable): String = {
    val exString = ex.toString
    exString match {
      case _ => ""
    }
  }

  def checkArgsNum(need: Int, actual: Int): Unit = {
    assert(actual == need, "参数个数不对:" + "需要" + need + "个参数，" + "实际接收" + actual + "个参数")
  }

  def viewDf(dataframe: DataFrame, dfName: String = "dataFrame"): Unit = {
    try{
      val firstLine: Array[String] = dataframe.dtypes.zip(dataframe.first().toSeq)
        .map(row => "[" + row._1._1 + "-" + row._1._2 + "-" + row._2 + "]")
      logger.info("**************" + dfName + "View*************")
      logger.info(dfName + ".size:" + dataframe.count())
      logger.info(firstLine.mkString(","))
      dataframe.show(20, false)
      logger.info("****************************************")
    }catch {
      case ex: Throwable => throw new Exception("DataFrame为空")
    }


  }
  //  //
  //  // 特殊性：
  //  // 1.更新 旧表 taskInstance 的状态,直接将状态置为hived,不必wms扫着建hive表，
  //  //2.查询父任务的hive放进自己的hive字段里面
  //  def updateFinalDB(tarskID: Int, header: String = "no head", resultJson: String = "no json"): Unit = {
  //
  //    val conn = conncetion()
  //    try {
  //      val getTableName =
  //        "SELECT TABLE_NAME FROM TB_DAE_TARSK_INSTANCE I WHERE I.ID = " +
  //          "(SELECT PARENT_ID FROM TB_DAE_TARSK_INSTANCE i where i.ID = " + tarskID + ")"
  //      val tableName = query(getTableName)
  //      val prep = conn.prepareStatement("UPDATE TB_DAE_TARSK_INSTANCE SET  INS_STATUS = ?,  TITILE_CONTENT = ?,DATATABLE_COLUNMS_LONGTEXT = ?,THEAD_LONGTEXT = ?,SUMMARY_JSON_CONTENT = ?,TABLE_NAME = ? WHERE ID = ?")
  //      prep.setString(1, "hived")
  //      prep.setString(2, getHiveHead(header))
  //      prep.setString(3, getColumns(header))
  //      prep.setString(4, getTableThead(header))
  //      prep.setString(5, resultJson)
  //      prep.setString(6, tableName)
  //      prep.setInt(7, tarskID)
  //      logger.info(prep.toString)
  //
  //      val result = prep.executeUpdate()
  //      logger.info("db update result:" + result)
  //    }
  //    finally {
  //      conn.close()
  //    }
  //  }

}
