package code.abandon

import java.io.{File, PrintWriter}

import scala.io.Source

/**
  * Created by asus on 2017/1/10.
  */
object ToJobserver {
  def main(args: Array[String]) {
    val basePath = "D:/cWS/daeSvn/src/main/scala/com/esoft/dae"
    val allDir = List[String]()
    println()
    val newAllDir = getDir(allDir, basePath).tail.map(_ + "/")
    newAllDir.foreach {
      dirPath => chooseMethod(dirPath)
    }
    //    chooseMethod("D:/cWS/daeSvn/src/main/scala/com/esoft/dae/dataSource")

  }

  def chooseMethod(dirPath: String): Unit = {
    //
    val dir = new File(dirPath)
    val outDir = new File(dirPath.replace("D:/cWS/daeSvn/src/main/scala/com/esoft/dae", "D:/cWS/DAE_V4/src/main/scala-2.10/com/esoft/dae"))
    if (dirPath.contains("abandon")) {

    } else {
      if (!outDir.exists()) {
        outDir.mkdirs()
      }
      dir.list().foreach { file =>
        println(file)
        if (new File(dir + "/" + file).isFile) {
          //是文件才执行操作
          val inFile = dir + "\\" + file
          val outFile = outDir + "\\" + file
          if (dirPath.contains("prediction"))
            //prediction包下的文件
            printPredictor(inFile, outFile)
          else if (dirPath.contains("dao/") || dirPath.contains("util/"))
            //工具类直接复制
            copy(inFile, outFile)
          else
          //其他通用的文件
            jobServerFommat(inFile, outFile)
        }
      }
    }
  }

  def jobServerFommat(inFile: String, outFile: String): Unit = {
    val codeList = Source.fromFile(inFile).getLines.toList
    val totalCode = codeList.mkString("\n")
    val pathDirs = inFile.split("\\\\")
    val start = pathDirs.indexOf("dae")

    val pack = "package com.esoft.dae." + pathDirs.slice(start + 1, pathDirs.length - 1).mkString(".")
    val objectName = pathDirs.last.substring(0, pathDirs.last.length - 6)

    val oriImportRow = codeList.filter(row => row.startsWith("import "))
    val newImportRow = oriImportRow.map(row => row.replace(".json4s.native.", ".json4s.jackson.")).mkString("\n")
    val oriExecRow = codeList.filter(row => row.contains("args(") && (!row.contains("//"))&&(!row.contains("case ex")))
    val newExecRow =
      (oriExecRow.head.replace("args(0),", "sc, args.head,") +: oriExecRow.tail).mkString("\n")
    val startIndex = codeList.indexWhere(row => row.contains("def") && row.contains("basePath"))
    val oriExecMethodRow = codeList.drop(startIndex)
    val newExecMethodRow =
      oriExecMethodRow.map {
        row =>
          row match {
            case ori if ori.contains("def") && ori.contains("basePath")
            => ori.replace("basePath", "sc: SparkContext, basePath")
            case ori if ori.contains("getContext") => "" //将获取context的语句去掉
            case ori => ori
          }
      }.mkString("\n")

    val w = new PrintWriter(new File(outFile))
    w.println(pack)
    w.println()
    //打印jobserver包的import语句
    w.println("import scala.util.Try\n" +
      "import spark.jobserver.api.{SingleProblem, ValidationProblem, JobEnvironment, SparkJob}\n" +
      "import org.scalactic._\n" +
      "import com.typesafe.config.Config")
    //打印算法的import语句
    w.println(newImportRow)
    w.println()
    w.println()
    w.println("/**\n" +
      "  * @author liuzw\n" +
      "  */")
    w.println("object " + objectName + " extends SparkJob with BaseDao {\n" +
      "  type JobData = Seq[String]\n" +
      "  type JobOutput = Unit\n")
    w.println("  def runJob(sc: SparkContext, runtime: JobEnvironment, args: JobData): JobOutput = {")
    w.println("    args.foreach(println(_))")
    w.println("    try")
    w.println(newExecRow)
    w.println("    catch{\n" +
      "      case ex: Throwable => handleException(args.last.toInt, ex, \"\")\n" +
      "    }")
    w.println("  }")
    w.println("  def validate(sc: SparkContext, runtime: JobEnvironment, config: Config):\n" +
      "  JobData Or Every[ValidationProblem] = {\n" +
      "    val a =\n" +
      "      Try(config.getString(\"input.string\").split(\"--\").toSeq)\n" +
      "    val b = a.map(words => Good(words))\n" +
      "    b.getOrElse(Bad(One(SingleProblem(\"No input.string param\"))))\n" +
      "  }")
    w.println(newExecMethodRow)
    w.close()
  }

  def printPredictor(inFile: String, outFile: String): Unit = {
    if (inFile.endsWith("prediction.scala")) {
      //      printPrediction(inFile,outFile)
    } else {
      val codeList = Source.fromFile(inFile).getLines.toList
      val (prefixList, suffixList) = codeList.filter(!containsIllegal(_)).
        splitAt(1 + codeList.indexWhere(row => row.contains("package")))
      val newSufList = "import org.apache.spark.SparkContext" +: suffixList
      val newCode = (prefixList ++ newSufList)
        .map(_.replace("basePath: String", "sc: SparkContext,basePath: String")).mkString("\n")
      val w = new PrintWriter(new File(outFile))
      w.write(newCode)
      w.close()
    }

  }

  def printPrediction(inFile: String, outFile: String): Unit = {
    val codeList = Source.fromFile(inFile).getLines.toList
    val pathDirs = inFile.split("\\\\")
    val start = pathDirs.indexOf("dae")

    val pack = "package com.esoft.dae." + pathDirs.slice(start + 1, pathDirs.length - 1).mkString(".")
    val objectName = pathDirs.last.substring(0, pathDirs.last.length - 6)

    val oriImportRow = codeList.filter(row => row.startsWith("import "))
    val newImportRow = oriImportRow.map(row => row.replace(".json4s.native.", ".json4s.jackson.")).mkString("\n")
    val oriExecRow = codeList.filter(row => row.contains("args(") && (!row.contains("//")))
    val newExecRow =
      (oriExecRow.head.replace("args(0),", "sc, args.head,") +: oriExecRow.tail).mkString("\n")
    val startIndex = codeList.indexWhere(row => row.contains("def") && row.contains("basePath"))
    val oriExecMethodRow = codeList.drop(startIndex)
    val newExecMethodRow =
      oriExecMethodRow.map {
        row =>
          row match {
            case ori if ori.contains("def") && ori.contains("basePath")
            => ori.replace("basePath", "sc: SparkContext, basePath")
            case ori if ori.contains("getContext") => "" //将获取context的语句去掉
            case ori => ori
          }
      }.mkString("\n")

    val w = new PrintWriter(new File(outFile))
    w.println(pack)
    w.println()
    //打印jobserver包的import语句
    w.println("import scala.util.Try\n" +
      "import spark.jobserver.api.{SingleProblem, ValidationProblem, JobEnvironment, SparkJob}\n" +
      "import org.scalactic._\n" +
      "import com.typesafe.config.Config")
    //打印算法的import语句
    w.println(newImportRow)
    w.println()
    w.println()
    w.println("/**\n" +
      "  * @author liuzw\n" +
      "  */")
    w.println("object " + objectName + " extends SparkJob with BaseDao {\n" +
      "  type JobData = Seq[String]\n" +
      "  type JobOutput = Unit\n")
    w.println("  def runJob(sc: SparkContext, runtime: JobEnvironment, args: JobData): JobOutput = {")
    w.println("    args.foreach(println(_))")
    w.println(newExecRow)
    w.println("  }")
    w.println("  def validate(sc: SparkContext, runtime: JobEnvironment, config: Config):\n" +
      "  JobData Or Every[ValidationProblem] = {\n" +
      "    val a =\n" +
      "      Try(config.getString(\"input.string\").split(\"--\").toSeq)\n" +
      "    val b = a.map(words => Good(words))\n" +
      "    b.getOrElse(Bad(One(SingleProblem(\"No input.string param\"))))\n" +
      "  }")
    w.println(newExecMethodRow)
    w.close()
  }

  def copy(inFile: String, outFile: String): Unit = {
    val codeList = Source.fromFile(inFile).getLines.toList.map {
      row => row.replace("class BaseDao", "trait BaseDao")
    }
    val newCode = codeList.mkString("\n")
    val w = new PrintWriter(new File(outFile))
    w.write(newCode)
    w.close()
  }

  def getDir(allDir: List[String], prefixPath: String): List[String] = {
    if (new File(prefixPath).isDirectory) {
      new File(prefixPath).list().foldLeft(allDir :+ prefixPath) {
        (oldDir, suffixPath) => getDir(oldDir, prefixPath + "/" + suffixPath)
      }
    } else
      allDir
  }

  def containsIllegal(row: String): Boolean = {
    row match {
      case i if row.contains("val sc = setSparkContext()") => true
      case i if row.contains("def setSparkContext(): SparkContext") => true
      case i if row.contains("}//illegal") => true
      case i if row.contains("SparkContext") && row.contains("import") => true
      case _ => false
    }
  }

  //  def getExec(codes:List[String]): String ={
  //
  //  }
}
