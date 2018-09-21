package code.abandon

import java.io.{File, PrintWriter}

import scala.io.Source

/**
  * Created by asus on 2017/2/21.
  */
object CopyToJobServer {


  def main(args: Array[String]) = {
    val basePath = "D:/cWS/daeSvn/src/main/scala/com/esoft/dae"

    val allDir = List[String]()
    println()
    val newAllDir = getDir(allDir, basePath).tail.map(_ + "/")
    newAllDir.foreach {
      dirPath =>
        //        copyToExecutor(dirPath)
        produceJobServer(dirPath)
    }
  }

  def copyToExecutor(dirPath: String): Unit = {
    val dir = new File(dirPath)
    val outDir = new File(dirPath.replace("D:/cWS/daeSvn/src/main/scala/com/esoft/dae", "D:/cWS/DAE_V4/src/main/scala-2.10/com/esoft/dae/worker"))
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
          if (dirPath.contains("dao/") || dirPath.contains("util/") || dirPath.contains("predictor")) {
            changeUtil(inFile, outFile) //修改包名，修改import
          } else {
            /*
            1.修改包名，修改import
            2.修改文件名，修改object名
            3.去掉main方法
            * */
            changeWorker(inFile, outFile)
          }
        }
      }

    }
  }

  def changeWorker(inFile: String, outFile: String): Unit = {
    println(inFile)
    val pathDirs = inFile.split("\\\\")
    val objectName = pathDirs.last.substring(0, pathDirs.last.length - 6)
    val start = pathDirs.indexOf("dae")
    val pack = "package com.esoft.dae.worker." + pathDirs.slice(start + 1, pathDirs.length - 1).mkString(".")

    val codeList = Source.fromFile(inFile).getLines.toList
      .filter(row => !row.contains("package com.esoft.dae"))
      .map(row => row.replace("com.esoft.dae", "com.esoft.dae.worker"))
    val resultList = pack +: codeList
    println(outFile)
    val w = new PrintWriter(new File(outFile))
    w.println(resultList.mkString("\n"))
    w.close()
  }

  def changeUtil(inFile: String, outFile: String): Unit = {
    println(inFile)
    val pathDirs = inFile.split("\\\\")
    val objectName = pathDirs.last.substring(0, pathDirs.last.length - 6)
    val start = pathDirs.indexOf("dae")
    val pack = "package com.esoft.dae.worker." + pathDirs.slice(start + 1, pathDirs.length - 1).mkString(".")

    val codeList = Source.fromFile(inFile).getLines.toList
      .filter(row => !row.contains("package com.esoft.dae"))
      .map(row => row.replace("com.esoft.dae", "com.esoft.dae.worker"))
    val resultList = pack +: codeList
    println(outFile)
    val w = new PrintWriter(new File(outFile))
    w.println(resultList.mkString("\n"))
    w.close()
  }

  def produceJobServer(dirPath: String): Unit = {
    val dir = new File(dirPath)
    val outDir = new File(dirPath.replace("D:/cWS/daeSvn/src/main/scala/com/esoft/dae", "D:/cWS/DAE_V4/src/main/scala-2.10/com/esoft/dae/jobserver"))
    if (dirPath.contains("abandon")) {
    } else {

      dir.list().foreach { file =>
        println(file)
        if (new File(dir + "/" + file).isFile) {
          //是文件才执行操作
          val inFile = dir + "\\" + file
          val outFile = outDir + "\\" + file
          if (dirPath.contains("dao/") || dirPath.contains("util/")) {
          }
          else if (dirPath.contains("prediction")) {
            //prediction包下只有prediction需要产生相应的jobserver类
            if (file == "prediction.scala") {
              if (!outDir.exists()) {
                outDir.mkdirs()
              }
              produce(inFile, outFile)
            }
          }
          else {
            //其他通用的文件
            if (!outDir.exists()) {
              outDir.mkdirs()
            }
            produce(inFile, outFile)
          }
        }
      }
    }
  }

  def produce(inFile: String, outFile: String): Unit = {
    println(outFile)
    val pathDirs = inFile.split("\\\\")
    val objectName = pathDirs.last.substring(0, pathDirs.last.length - 6)
    val start = pathDirs.indexOf("dae")
    val pack = "package com.esoft.dae.jobserver." + pathDirs.slice(start + 1, pathDirs.length - 1).mkString(".")
    val w = new PrintWriter(new File(outFile))
    w.println(pack)
    w.println()
    w.println("import scala.util.Try\n" +
      "import spark.jobserver.api.{SingleProblem, ValidationProblem, JobEnvironment, SparkJob}\n" +
      "import org.scalactic._\n" +
      "import com.typesafe.config.Config\n" +
      "import org.apache.spark.SparkContext\n\n\n" +
      "/**\n" +
      "  * @author liuzw\n" +
      "  */\n")
    w.println("object " + objectName + " extends SparkJob  {")
    w.println("  type JobData = Seq[String]\n" +
      "  type JobOutput = Unit\n\n" +
      "  def runJob(sc: SparkContext, runtime: JobEnvironment, args: JobData): JobOutput = {")
    val execPath = "    com.esoft.dae." +
      pathDirs.slice(start + 1, pathDirs.length - 1).mkString(".") + "." + objectName + ".exec(sc,args.toArray)"
    w.println(execPath)
    w.println("  }\n" +
      "  def validate(sc: SparkContext, runtime: JobEnvironment, config: Config):\n" +
      "  JobData Or Every[ValidationProblem] = {\n" +
      "    val a =\n" +
      "      Try(config.getString(\"input.string\").split(\"--\").toSeq)\n" +
      "    val b = a.map(words => Good(words))\n" +
      "    b.getOrElse(Bad(One(SingleProblem(\"No input.string param\"))))\n" +
      "  }\n\n}")
    //    com.esoft.dae.executor.dataSource.DataSourceParquet.exec(sc,args.toArray)
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
}
