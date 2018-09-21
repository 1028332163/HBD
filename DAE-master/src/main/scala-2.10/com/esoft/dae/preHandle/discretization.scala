package com.esoft.dae.preHandle

import com.esoft.dae.dao.BaseDao
import com.esoft.dae.util.handleUtil
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.Bucketizer
import org.apache.spark.sql.{SaveMode, SQLContext}
import org.apache.spark.sql.functions._
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

/**
  *
  * @author liuzw
  *
  *         数据离散化
  *
  */
class discretization(taskId: String) extends BaseDao(taskId) {
  ///////界值的处理还待商议
  def discretization(sc: SparkContext, basePath: String, inputPath: String,
                     splitBaseDistance: String, colNames: String, splitNum: String,
                     outputPath: String, taskId: String) {
    val sqlContext = new SQLContext(sc)
    val dataFrame = sqlContext.read.parquet(basePath + inputPath)
    //    val dataFrame = csvUtil.readCsv(sc, sqlContext, basePath + inputPath)
    viewDf(dataFrame, "dataFrame")

    ///////////////离散化参数转化成map
    val splitParaMap: Array[(String, Int)]
    = colNames.split(",").map(colName => (colName, splitNum.toInt))

    val dfSize = dataFrame.count

    val iniVOs = new Array[VO](0)
    implicit lazy val formats = Serialization.formats(NoTypeHints)

    val (finalVOs, finalDf) = splitParaMap.foldLeft((iniVOs, dataFrame)) {
      //      (tuple1,tuple2) => ..... return (tuple3,tuple4)
      // oldVos = tuple1._1 df = tuple1._2
      case (((oldVOs, df)), (splitCol, splitCount)) =>
        //构造由分界点组成的数组，并初始化最小值和最大值
        logger.info("splitcol " + splitCol + " to " + splitCount)
        val colMin =
          try {
            df.na.drop(Array(splitCol)).select(min(splitCol)).first.get(0).toString.toDouble
          } catch {
            case ex: Throwable => 0.0
          }

        val colMax =
          try {
            df.na.drop(Array(splitCol)).select(max(splitCol)).first.get(0).toString.toDouble
          } catch {
            case ex: Throwable => 0.0
          }

        val splitNums = new Array[Double](splitCount + 1)
        splitNums(0) = colMin
        splitNums(splitCount) = colMax
        if (splitBaseDistance.toBoolean) {
          //如果是等距离散化需要求出跨度是多少
          val gap = (colMax - colMin) / splitCount
          for (i <- 1 until splitCount) {
            splitNums(i) = colMin + i * gap
          }
        } else {
          //如果是等频率离散化要先将列按大小排序，然后求出等频率每个区间的个数（也就是角标的变化值），根据角标获取划分界值
          //将要离散化的列排序以得到分割频率上的点(转换成数组形式方便用角标访问）
          val perfectDfCol = dataFrame.select(splitCol).na.drop
          val splitIndex = for (i <- 1 until splitCount)
            yield (((dfSize - 1) * i) / splitCount.toDouble).round
          val splitNumDf = handleUtil.filteByIndex(perfectDfCol.orderBy(splitCol), splitIndex)
          val splitNumArr = handleUtil.getCol(splitNumDf)
          for (i <- splitNumArr.indices) {
            splitNums(i + 1) = splitNumArr(i).asInstanceOf[Double]
          }
        }
        val uniSplitNums = handleEqual(splitNums, df.count())
        //调用API分割数据
        val outputcol = "discrete_" + splitCol
        val bucketizer = new Bucketizer()
          .setInputCol(splitCol)
          .setOutputCol(outputcol)
          .setSplits(uniSplitNums)
        logger.info(splitCol + " oriSplitArr:" + splitNums.mkString(","))
        logger.info(splitCol + " newSplitArr:" + uniSplitNums.mkString(","))
        (oldVOs :+ VO(splitCol, Serialization.write(uniSplitNums)), bucketizer.transform(df))
    }
    viewDf(finalDf)
    //页面json
    val json = Serialization.write(finalVOs)
    logger.info("finalJson:" + json)
    //写入数据
    finalDf.write.format("parquet").mode(SaveMode.Overwrite).save(basePath + outputPath)
    super.flagSparked(taskId.toInt, outputPath, handleUtil.getHeadContent(finalDf.dtypes), json)

  }

  def handleEqual(oriArr: Array[Double], dfLength: Long): Array[Double] = {

    assert(oriArr.head != oriArr.last, "此列只有一个值，不能进行离散化")
    //如果连续的两个分隔数相等，第二个分隔数预设的右移大小step
    val step = Math.min(0.000000001, (oriArr.last - oriArr.head) / dfLength)
    val newArr = new Array[Double](oriArr.length)
    for (i <- newArr.indices) {
      newArr(i) =
        i match {
          //第一个分隔数不必做任何处理
          case 0 => oriArr(i)
          //最后的一个分隔数后移step
          case last if i == oriArr.length - 1 =>
            if (oriArr(i) == oriArr(i - 1))
              oriArr(i) + step
            else oriArr(i)
          //中间的分隔数
          case _ =>
            if (oriArr(i) <= newArr(i - 1)) {
              newArr(i - 1) + step
            }
            else oriArr(i)

        }
    }
    newArr


  }

  case class VO(colName: String, splitNums: String)

}

object discretization {

  def main(args: Array[String]) {
    val basePath = ""
    val inputPath = "data/treeRgs.csv"

    val featuresCol = "col1,col2" //要离散化哪几列
    val splitNum = "2"
    val splitBaseDistance = "false" //distance(true) OR frequency(false)

    val head = ""
    val outputPath = "out/lzw_standardScale_Out.parquet"
    val taskID = "815"
    exec(handleUtil.getContext("discretization"), args)

  }

  def exec(sc: SparkContext, args: Array[String]): Unit = {
    implicit lazy val formats = org.json4s.jackson.Serialization.formats(org.json4s.NoTypeHints)
    case class ArgVO(inputPath: String,
                     splitBaseDistance: String, featuresCol: String, splitNum: String,
                     outputPath: String)

    val executer = new discretization(args.last)
    try {
      args.foreach { x => executer.logger.info("inArg-" + x) }
      val basePath = args(0)
      val ArgVO(inputPath: String,
      splitBaseDistance: String, colNames: String, splitNum: String,
      outputPath: String)
      = org.json4s.jackson.Serialization.read[ArgVO](args(1))
      val taskId = args(2)
      //executer.checkArgsNum(7, args.length)
      executer.discretization(sc: SparkContext, basePath: String, inputPath: String,
        splitBaseDistance: String, colNames: String, splitNum: String,
        outputPath: String, taskId: String)
    } catch {
      case ex: Throwable => executer.handleException(args(args.length - 1).toInt, ex, "discretization")
    }
  }

}
