import java.sql.Timestamp

import com.esoft.dae.dataSource.DataSourceParquet
import com.esoft.dae.preHandle.missingValueReplace
import com.esoft.dae.util.handleUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, Row, SQLContext}
import org.apache.spark.sql.types.StructType
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by asus on 2017/4/24.
  */
object RunMyMethod {
  val sc = new SparkContext(new SparkConf().setAppName("debug").setMaster("local"))
  val sqlContext = new SQLContext(sc)

  def main(args: Array[String]) {
    missingValueReplace()
  }

  def missingValueReplace(): Unit = {
    val basePath = "data/MissingValueReplace/"
    val inputPath = "test.parquet"
    val exeDrop = "true"
    val dropCols = "col1,col2,label"
    val maxNullPro = "0.6"
    val exeReplace = "true"
    val strCols = "labelstr"
    val strBlank = "true"
    val strUD = "d"
    val strNew = "aaa"
    val numCols = "col1,col2"
    val numNaN = "true"
    val numUD = "10"
    val numNew = "Mean"
    val outputPath = "out"
    val taskId = "0"

    val executer = new missingValueReplace(taskId)

    executer.execMissingValueHandle(sc: SparkContext, basePath: String, inputPath: String,
      exeDrop: String, dropCols: String, maxNullPro: String,
      exeReplace: String,
      strCols: String, strBlank: String, strUD: String, strNew: String,
      numCols: String, numNaN: String, numUD: String, numNew: String,
      outputPath: String, taskId: String)
  }

  def binaryClassEval(): Unit = {
    val basePath = "" //基本路径
    val inputPath = "data/evaluate.csv" //评估数据集的路径
    val probabilityCol = "probability" //
    val predictionCol = "prediction"
    val labelCol = "label" //原先的标签列
    val outputPath = "" //
    val taskId = "1" //
  }

  def multiClassEval(): Unit = {
    val basePath = "" //基本路径
    val inputPath = "data/evaluate.csv" //评估数据集的路径
    val predictionCol = "prediction"
    val labelCol = "label" //原先的标签列
    val outputPath = ""
    val taskId = "1" //
  }

  def mkParquet(): Unit = {
    val basePath = "data/MissingValueReplace/"
    val inCsv = "1.csv"
    val inHead = "rownum int," +
      "col1 double,col2 double,label double,labelstr string"
    val inParquet = "test.parquet"
    csvToParquet(basePath + inCsv, basePath + inParquet, inHead)
  }

  def csvToParquet(inputPath: String, outputPath: String, head: String): Unit = {

    val input: RDD[String] = sc.textFile(inputPath)
    val rddWithOutHead: RDD[(String, Long)] =
      input.zipWithIndex.filter {
        //给数据集添加索引，并删除第一条表头和没数据的行
        line => line._2 != 0
      }
    val resultRDD: RDD[String] = rddWithOutHead.map {
      //将行号放到前面
      line =>
        line._2 + "," + line._1
    }

    //解析hive 表的表头
    val schema: StructType = handleUtil.getSchema(head)
    val headArr = head.split(",")
    //得到类型的RDD
    val typeArray: Array[String] = headArr.map { x => x.split(" ")(1) }
    //[int,int]
    //将RDD转换成ROW
    val dataInRow: RDD[Row] = resultRDD.map { row =>
      //csvData为从文件中读取的数据
      val csvData = row.split(",")
      val rowData: Array[String] =
        if (headArr.length == csvData.length)
          csvData
        else {
          Range(0, headArr.length - csvData.length).foldLeft(csvData)((arr, i) => arr :+ "")
        }
      //类型和数据对应
      //[1,a]=>[(1,int),(a,string)]
      val mapArray: Array[(String, String)] = rowData.zip(typeArray)
      Row(mapArray.map { x =>
        x._2 match {
          case "int" => try {
            //replace是为了处理  "1"  这种被引号括起来的情况
            x._1.toString.replace("\"", "").toInt
          } catch {
            case ex: Exception => null
          }
          case "string" =>
            try {
              if ("" == x._1.toString.trim)
                null
              else
                x._1.toString
            } catch {
              case ex: Exception => null
            }
          case "double" => try {
            x._1.toString.replace("\"", "").toDouble
          } catch {
            case ex: Exception => null
          }
          case "timestamp" => try {
            Timestamp.valueOf(x._1.toString)
          } catch {
            case ex: Exception => null
          }
        }
      }: _*) //_*表示将集合所有的内容构造对象
    }
    //将Schema应用于RDD
    val dataFrame = sqlContext.createDataFrame(dataInRow, schema)
    dataFrame.show()
    dataFrame.write.format("parquet")
      .mode(SaveMode.Overwrite)
      .save(outputPath)
  }

  def mkData(): Unit = {
    val dataFrame = sqlContext.createDataFrame(Seq(
      (0.0, 0.1, 0.1, 0.1, 0.1),
      (1.0, 0.2, 0.1, 0.1, 0.1),
      (2.0, 0.3, 0.1, 0.1, 0.1)
    )).toDF("col1", "col2", "col3", "col4", "col5")
  }
}
