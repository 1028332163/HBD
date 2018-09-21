package code

import java.io.{File, PrintWriter}

/**
  * Created by ASUS on 2016/11/24.
  * 输入参数可以在项目中直接生成代码
  */
object CodeMaker {
  val basePath = "D:/cWS/daeSvn/src/main/scala/com/esoft/"

  def main(args: Array[String]): Unit = {
    val pack = "dae.preHandle"//代码要放在哪个包下，例子：com.esoft.dae.prediction填写dae.prediction
    val fileName = "stratifiedSampling"//scala的文件名，object的名字
    val modelPack = "StratifiedSampling"//创建的模型在spark哪个包里面
    val paraNames = "labelCol,fractionNum,fractionPro,seed"
    writerCode(pack: String,fileName:String, modelPack: String, paraNames: String)
  }

  def writerCode(pack: String,fileName:String, modelPack: String, paraNames: String) {
    val paraArr = paraNames.split(",")//分割参数
    val modelName = modelPack.split("\\.").last
    //创建输出路径
    val dirPath = basePath + pack.split("\\.").mkString("/")
    val dir = new File(dirPath)
    if(!dir.exists()){
      dir.mkdirs()
    }
    val w = new PrintWriter(new File(dirPath+"/"+fileName+".scala"))
    //写包名
    w.println("package com.esoft."+pack)
    //写import语句
    w.println("import com.esoft.dae.dao.BaseDao\n" +
      "import org.apache.spark.{SparkConf, SparkContext}\n" +
      "import com.esoft.dae.util.csvUtil\n" +
      "import org.apache.spark.mllib.linalg.{VectorUDT, Vectors}\n" +
      "import org.apache.spark.mllib.linalg.Vector\n" +
      "import org.apache.spark.sql.{SaveMode,SQLContext}\n" +
      "import org.json4s.NoTypeHints\n" +
      "import org.json4s.jackson.Serialization._\n" +
      "import org.json4s.jackson.Serialization")
    w.println("import " + modelPack)
    w.println("/**\n  * @author \n  */")
    w.println("object " + fileName + " extends BaseDao {")
    w.println("def main(args: Array[String]): Unit = {")
    //定义变量语句
    w.println("val basePath = \"\"\n" +
      "val inputPath = \"\"")
    w.println()
    paraArr.foreach(col => w.println("val " + col + "=\"" + "\""))
    w.println()
    w.println("val outputPath = \"\"\n" +
      "val taskId = \"182\"")
    w.println()

    //函数的调用语句
    w.println("exec" + modelName + "(basePath, inputPath,")
    w.println(paraNames+",")
    w.println("outputPath, taskId: String)")

//    w.println(" args.foreach { x => executer.logger.info("inArg:"+x) }")
//    w.println("//    exec" + modelName + "(args:_*)")
    w.println("//    exec" + modelName + "(args(0), args(1),")
    w.print("//      ")
    for (i <- paraArr.indices) {
      w.print("args(" + (i + 2) + "),")
    }
    w.print("\n")
    w.println("//      args(" + (paraArr.length+2) + "), args(" + (paraArr.length+3) + "))\n}")
    //主函数结束，开始算法函数的定义
    w.println("def " + "exec" + modelName + "(basePath: String, inputPath: String,")
    paraArr.foreach(para => w.print(para + ":String,"))
    w.print("\n")
    w.println("outputPath: String, taskId: String): Unit = {")
    //读文件的语句
    w.println("    ///////////构建yarn-spark,从parquet中读取文件\n    " +
      "//    val sc = getContext(\"" + modelName + "\")\n    " +
      "//    val sqlContext = new SQLContext(sc)\n    " +
      "//    val dataFrame = sqlContext.read.parquet(basePath + inputPath)\n    " +
      "//////////构建local-spark,从csv中读取文件\n    " +
      "val conf = new SparkConf().setAppName(\"test\").setMaster(\"local\")\n    " +
      "val sc = new SparkContext(conf)\n    " +
      "val sqlContext = new SQLContext(sc)\n    " +
      "val dataFrame = csvUtil.readCsv(sc, sqlContext, basePath + inputPath)\n    " )
    w.println("//////////////////参数处理")
    w.println("//val finalCols = addCols.split(\",\"):+ predictionCol//可以用在finalDf的select方法中，对结果的列进行筛选")
    w.println("//val dfToUse  = this.combineCol(sqlContext,dataFrame,featuresCol)//可以将输入的多个特征列放到fetures向量中")
    w.println("val dfToUse  = dataFrame")
    w.println("//val finalHead = headContent + \",\" + predictionCol + \" int\"//如果算法改变了df的列结构，则需要重新处理headContent")
    w.println("////////////////////将算法作用于数据集")
    w.println("val " + tolow(modelName) + "= new " + toUp(modelName) + "()")
    paraArr.foreach(para => w.println("//.set" + toUp(para) + "(" + para + ")"))
    w.println("val fitModel = " + tolow(modelName) + ".fit(dfToUse)")
//    w.println("val finalDf = fitModel.transform(dfToUse).select(finalCols.head, finalCols.tail: _*)\n" +
//      "finalDf.show()")
    w.println("val finalDf = fitModel.transform(dfToUse)")
    w.println("//.select(finalCols.head, finalCols.tail: _*)")
    w.println("finalDf.show()")
    w.println("////////////////////页面展现用的df")
    w.println("//    val showDf = finalDf.select(predictionCol, addCols.split(\",\"): _*)\n" +
      "//    showDf.show()\n" +
      "//    val showDfPath = getDfShowPath(outputPath) + \"1.parquet\"\n" +
      "//    showDf.write.format(\"parquet\")\n" +
      "//      .mode(SaveMode.Overwrite)\n" +
      "//      .save(basePath + showDfPath)\n" +
      "//    updateTaskInsData(taskId.toInt, showDfPath, showDf.dtypes, ONLY_ONE_TABLE, \"no json\")")
    w.println("    ////////////////////保存结果及保存json\n" +
      "//    fitModel.write.overwrite().save(basePath + modelSavePath)\n" +
      "//\n" +
      "//    val showDfPath = getDfShowPath(outputPath) + \"1.parquet\"\n" +
      "//    showDf.write.format(\"parquet\")\n" +
      "//      .mode(SaveMode.Overwrite)\n" +
      "//      .save(basePath + showDfPath)\n" +
      "//    updateTaskInsData(taskId.toInt, showDfPath, showDf.dtypes, ONLY_ONE_TABLE, \"no json\")\n" +
      "//    super.updateDB(taskId.toInt, outputPath, \"no head\", \"no json\")")
    w.println("    case class FinalVO(tcols:Array[String],hreshHold:Double,intercept:Double,weight:Vector)\n    " +
      "implicit lazy val formats = Serialization.formats(NoTypeHints)\n    " +
      "val json = write(FinalVO(featuresCol.split(\",\"),fitModel.getThreshold.get,fitModel.intercept,fitModel.weights))\n" +
      "    println(json)")

    w.println("  }\n\n}")
    w.close()
  }

  def tolow(str: String): String = {
    //字符串的第一个转为小写
    val first = str.substring(0, 1).toLowerCase
    val tail = str.substring(1, str.length)
    first + tail
  }

  def toUp(str: String): String = {
    //字符串的第一个转为大写
    val first = str.substring(0, 1).toUpperCase
    val tail = str.substring(1, str.length)
    first + tail
  }
}
