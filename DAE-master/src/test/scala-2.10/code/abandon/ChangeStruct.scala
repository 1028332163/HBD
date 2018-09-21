package code.abandon

import java.io.File

import scala.io.Source

/**
  * Created by asus on 2017/2/25.
  */
object ChangeStruct {
  def main(args: Array[String]) {
    val basePath = "D:/cWS/DAE_V4/src/main/scala-2.10/com/esoft/dae/"
    val allDir = List[String]()
    //    val newAllDir = getDir(allDir, basePath).tail.map(_ + "/")
    //    newAllDir.foreach {
    //      dirPath => chooseMethod(dirPath)
    //    }
    chooseMethod("D:/cWS/DAE_V4/src/main/scala-2.10/com/esoft/dae/evaluate")

  }

  def chooseMethod(dirPath: String): Unit = {
    //dirPath的路径分隔符是“/”   dir的路径分隔符是"\\"
    val dir = new File(dirPath)
    val outDir = dir
    if (!outDir.exists()) {
      outDir.mkdirs()
    }
    dir.list().foreach { file =>
      val inFile = dir + "\\" + file
      if (new File(inFile).isFile && needChange(inFile)) {
        //是文件才执行操作比并且需要进行改变
        val outFile = outDir + "\\" + file
        change(inFile, outFile)
      }
    }

  }

  def change(inFile: String, outFile: String): Unit = {
    val codeList = Source.fromFile(inFile).getLines.toList
    val pathDirs = inFile.split("\\\\")
    val start = pathDirs.indexOf("dae")

    val pack = "package com.esoft.dae." + pathDirs.slice(start + 1, pathDirs.length - 1).mkString(".")
    val objectName = pathDirs.last.substring(0, pathDirs.last.length - 6)
    val ori = codeList.map{row=>
      if(row.contains(" extends BaseDao")){
        row.replace("object","class").
          replace(" extends BaseDao","(taskId: String) extends BaseDao(taskId) ")
      }else row
    }
    val (use1,overplus) = ori.splitAt(ori.indexWhere(_.contains("def main")))
    val (main,overplus1) = overplus.splitAt(overplus.indexWhere(row=>row.contains("def")
      &&row.contains("sc: SparkContext, basePath: String")))
    val (use2,overplus2) = overplus1.splitAt(overplus1.indexWhere(_.contains("def exec(sc: SparkContext, args: Array[String]): Unit")))
    val (oriExec, use3) = overplus2.splitAt(3 + overplus2.indexWhere(_.contains("case ex: Throwable => handleException(")))
    val exec = handleExec(oriExec)
    val objectCode = ((("object "+objectName+" {\n")+:main)++ exec):+"\n}"

    println(use1.mkString("\n"))
    println("++++++++++++++++++++++++++++++++++")
    println(use2.mkString("\n"))
    println("++++++++++++++++++++++++++++++++++")
    println(use3.mkString("\n"))
    println("++++++++++++++++++++++++++++++++++")
    println(objectCode.mkString("\n"))

    val finalCode = use1 ++ use2 ++ main ++ exec
  }
  def handleExec(ori: List[String]): List[String] = {
    ori.map{
      row=>
        if(row.contains("def exec(sc: SparkContext, args: Array[String]): Unit"))
          row + "\n" + "    val executer = new DataSourceParquet(args.last)"
        else if(row.contains("sc, args(0), args(1),"))
          "executer."+row.trim
        else
          row.replace("checkArgsNum","//executer.checkArgsNum").replace("handleException","executer.handleException")
    }
  }

  def needChange(inFile: String): Boolean = {
    if (!inFile.contains("\\dao\\")
      && !inFile.contains("\\util\\")
      && !inFile.contains("\\jobserver\\")
      && !inFile.contains("\\predictor\\")
      && !inFile.contains("\\dataSource\\")
    )
      true
    else
      false
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
