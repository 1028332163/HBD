package com.esoft.dae

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, DataFrame, SQLContext}
import com.esoft.dae.util.{handleUtil, csvUtil}

/**
  * Created by asus on 2017/3/15.
  */
object TestUtil {


  val sqlContext = new SQLContext(ContextContainer.sc)

  def compareDf(dfPath1: String, dfPath2: String): Boolean = {
    println("dfPath1" + dfPath1)
    println("dfPath2" + dfPath2)
    //    true
    val df1 = sqlContext.read.parquet(dfPath1)
    df1.show
    val df2 = sqlContext.read.parquet(dfPath2)
    df2.show
    compareDf(df1, df2)
  }

  //判断两个df是否一样(一定要有唯一标示的rownum，如果df中有两行相同,join后的数量便不一样啦)
  private def compareDf(df1: DataFrame, df2: DataFrame): Boolean = {
    if (df1.count != df2.count)
      false
    else if (!df1.dtypes.sameElements(df2.dtypes))
      false
    else {
      //在join的时候 null！=null 需要用“” 和 0 替换null值
      val joinDf = df1.na.fill("").na.fill(0.0).join(df2.na.fill("").na.fill(0.0), df1.columns)
      if (df1.count == joinDf.count)
        true
      else
        false
    }
  }

  def compareNoSeqDf(dfPath1: String, dfPath2: String): Boolean = {
    println("dfPath1" + dfPath1)
    println("dfPath2" + dfPath2)
    //    true
    val df1 = sqlContext.read.parquet(dfPath1)
    df1.show
    val df2 = sqlContext.read.parquet(dfPath2)
    df2.show
    compareNoSeqDf(df1, df2)
  }

  //判断两个df是否一样(一定要有唯一标示的rownum，如果df中有两行相同)
  private def compareNoSeqDf(df1: DataFrame, df2: DataFrame): Boolean = {
    if (df1.count != df2.count)
      false
    else if (!df1.dtypes.sameElements(df2.dtypes))
      false
    else {
      //在join的时候 null！=null 需要用“” 和 0 替换null值
      val sortCols = df1.columns.tail
      val dfLeft = handleUtil.indexDataFrame(df1.na.fill("").na.fill(0.0).sort(sortCols.head, sortCols.tail: _*))
      dfLeft.show
      val dfRight =
        handleUtil.indexDataFrame(df2.na.fill("").na.fill(0.0).sort(sortCols.head, sortCols.tail: _*))
      dfRight.show
      val joinDf = dfLeft.join(dfRight, df1.columns)
      if (df1.count == joinDf.count)
        true
      else
        false
    }
  }

}
