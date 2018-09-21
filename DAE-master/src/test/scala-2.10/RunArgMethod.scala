import com.esoft.dae.dataSource.DataSourceParquet
import com.esoft.dae.evaluate.mllib.{mutiClassEval, binaryClassEval}
import com.esoft.dae.model.{kmeansCluster, naiveBayesClassifier, lineR}
import com.esoft.dae.prediction.prediction
import com.esoft.dae.util.csvUtil
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by asus on 2017/3/27.
  */
object RunArgMethod {
  def main(args: Array[String]) {
    val runArgs = handleArg()
    val sc = new SparkContext(new SparkConf().setAppName("debug").setMaster("local"))
    mutiClassEval.exec(sc,runArgs)

    //    binaryClassEval.exec(sc,runArgs)//二分类
  }
  //调bug的时候使用
  def handleArg(): Array[String] = {
    val arg = "l.scala:88)] [] - inArg-hdfs://192.168.88.10:8020/\n[2017-04-28 17:10:35,676] INFO  [com.esoft.dae.evaluate.mllib.mutiClassEval$$anonfun$exec$1.apply(mutiClassEval.scala:88)] [] - inArg-{\"predictionCol\":\"numpregnancies\",\"labelCol\":\"class\",\"inputPath\":\"out/20170428/p777_1493369451817.parquet\",\"outputPath\":\"out/20170428/p777_1493369451820.parquet\"}\n[2017-04-28 17:10:35,677] INFO  [com.esoft.dae.evaluate.mllib.mutiClassEval$$anonfun$exec$1.apply(mutiClassEval.scala:88)] [] - inArg-6733"
    arg.split("\n").map {
      row => row.split("inArg-")(1)
    }
  }
}
