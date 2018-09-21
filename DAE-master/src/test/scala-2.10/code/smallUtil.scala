package code

/**
  * Created by asus on 2016/12/5.
  */
object smallUtil {
  def main(args: Array[String]) {
    //    val para ="    val fractionNum = \"5\"\n    val fractionPro = \"0.5\"//每个样本被抽中的概率，所以最终的样本数会在size*fractionPro值附近波动\n    val seed = \"null\"\n    val withReplacement = \"false\""
    //    paraToVo(para)
    val arg = "{\n  \"duration\": \"Job not done yet\",\n  \"classPath\": \"com.esoft.dae.dataSource.DataSourceParquet\",\n  \"startTime\": \"2017-01-17T15:18:46.903+08:00\",\n  \"context\": \"test\",\n  \"status\": \"STARTED\",\n  \"jobId\": \"14b9c890-aadd-4e76-b6a9-395ac5d8ffaa\"\n}"
    toArgArray(arg)
  }

  def toArgArray(arg: String) {
    println(arg.split("\n").mkString("args = Array(\"", "\",\"", "\")"))
  }

  //根据scala代码中的para参数，生成wms中的VO
  def paraToVo(para: String): Unit = {
    val paraArr = para.split(" val").filter(para => para.trim != "").map(para =>
      if (para.indexOf("//") == -1) {
        //        println(para)
        "String " + para.substring(0, para.indexOf("=")) + ";\n"
      } else {
        //        println(para)
        "String " + para.substring(0, para.indexOf("=")) + ";" + para.substring(para.indexOf("//"))
      })
    paraArr.foreach(println(_))
  }
}
