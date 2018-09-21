package code.abandon

import java.io.File

/**
  * Created by asus on 2017/4/5.
  */
object Util {
  def getDir(allDir: List[String], prefixPath: String): List[String] = {
    if (new File(prefixPath).isDirectory) {
      new File(prefixPath).list().foldLeft(allDir :+ prefixPath) {
        (oldDir, suffixPath) => getDir(oldDir, prefixPath + "/" + suffixPath)
      }
    } else
      allDir
  }
}
