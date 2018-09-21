package code.abandon

import java.io.{File, PrintWriter}

import scala.io.Source

/**
  * Created by asus on 2017/4/5.
  */
object ToCaseVo {
  def main(args: Array[String]) {
    val basePath = "D:/cWS/DAE_V4/src/main/scala-2.10/com/esoft/dae"
    val allDir = List[String]()
    println()
    val newAllDir = Util.getDir(allDir, basePath).tail.map(_ + "/")
    newAllDir.foreach {
      dirPath => chooseMethod(dirPath)
    }
  }
  def chooseMethod(dirPath: String): Unit = {
    //
    val dir = new File(dirPath)
    val outDir = dir
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
          if (dirPath.contains("dao/") || dirPath.contains("util/")|| dirPath.contains("dataSource/")
            ||dirPath.contains("jobserver/")||dirPath.contains("prediction/"))
            {}
          else
          //其他通用的文件
            change(inFile, outFile)
        }
      }
    }
  }

  def change(inFile: String, outFile: String): Unit = {
    val codeList = Source.fromFile(inFile).getLines.toArray
    val head = codeList.slice(
      codeList.indexWhere(row => row.contains("sc: SparkContext, basePath: String")),
      codeList.indexWhere(row => row.contains("outputPath: String,")) + 1)
      .mkString("\n")
    val arg = head.substring(head.indexOf("input"), head.indexOf("taskI") - 2)
    val methodIndex = codeList.indexWhere(row => row.contains("def")&&row.contains("exec(sc: SparkContext"))
    codeList(methodIndex) = codeList(methodIndex) + "\n" +
      "    implicit lazy val formats = org.json4s.jackson.Serialization.formats(org.json4s.NoTypeHints)\n" +
      "    case class ArgVO(" + arg + ")\n"
    val tryIndex = codeList.indexWhere(row => row.contains("args.foreach"), methodIndex)
    codeList(tryIndex) = codeList(tryIndex) + "\n" +
      "      val basePath = args(0)" + "\n" +
      "      val ArgVO(" + arg + ")\n" +
      "      = org.json4s.jackson.Serialization.read[ArgVO](args(1))\n" +
      "      val taskId = args(2)"
    val execIndex = codeList.indexWhere(row=>row.contains("executer.")&&row.contains("sc"),methodIndex+1)
    val str = codeList(execIndex)
    codeList(execIndex) = str.substring(0,str.indexOf("sc"))+"sc: SparkContext, basePath: String, "+
      arg + ", taskId: String)"
    var tmp = execIndex+1
    println(codeList(tmp))
    while(codeList(tmp).contains("args(")){
      codeList(tmp) = ""
      tmp = tmp+1
    }
    val w = new PrintWriter(new File(outFile))
    w.println(codeList.mkString("\n"))
    w.close()
  }
}

