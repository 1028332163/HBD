import java.io.{File, PrintWriter}
import java.sql.Timestamp
import com.databricks.spark.csv.util.TextFile
import com.esoft.dae.dao.BaseDao
import com.esoft.dae.util.{handleUtil, csvUtil}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.{PipelineStage, Pipeline, PipelineModel}
import org.apache.spark.ml.classification.{GBTClassifier, LogisticRegression, NaiveBayes, DecisionTreeClassifier}
import org.apache.spark.ml.feature._
import org.apache.spark.ml.regression.{LinearRegression, DecisionTreeRegressionModel, DecisionTreeRegressor}
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.clustering.GaussianMixture
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql.catalyst.expressions.ScalaUDF
import org.apache.spark.sql.types.{StringType, TimestampType, StructType, StructField}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD
import com.databricks.spark.csv._

import scala.collection.immutable.IndexedSeq

/**
  * Created by asus on 2016/12/6.
  */
object mlTest {


  def main(args: Array[String]) {
    //    val l = new SVMWithSGD
    //    println("" +
    //      l.isAddIntercept
    //    )
    exeTest()

    println("(mlTest.scala:27)")
  }


  def exeTest(): Unit = {
    val conf = new SparkConf().setAppName("test").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val basePath = "hdfs://192.168.88.10:8020/"
    val inputPath = "out/20170412/p641_1491960701761.parquet"

    val probabilityCol = "probability"
    val predictionCol = "prediction"
    val labelCol = "label"

    val dataFrame = sqlContext.createDataFrame(Seq(
      (0.7, "0", 1),
      (0.8, "0", 1),
      (0.1, "1", 0),
      (0.2, "1", 0)
    )).toDF(probabilityCol, predictionCol, labelCol)
    "SELECT ID, PROVINCE_NAME provinces, CITY_NAME city, STATION_NAME station, IS_AFFRIM isAffrim, PILE_ID pile, FAULT_TYPE faultType, FAULT_START_TIME faultTime, ifnull(    TIMESTAMPDIFF(       MINUTE,       FAULT_START_TIME,       ifnull(FAULT_END_TIME, SYSDATE())    ),    '' ) faultTimer, FAULT_DETAILS faultDetails, ifnull(WORK_ORDER_ID, '') orderId, ifnull(WORK_ORDER_STATE, '') orderState, ifnull(WORK_ORDER_START_TIME, '') orderPayoutTime, ifnull(    TIMESTAMPDIFF(       MINUTE,       WORK_ORDER_START_TIME,       ifnull(          WORK_ORDER_END_TIME,          SYSDATE()       )    ),    '0' ) orderElapsedTime, CASE WHEN ifnull( TIMESTAMPDIFF(    MINUTE,    WORK_ORDER_START_TIME,    ifnull(       WORK_ORDER_END_TIME,       SYSDATE()    ) ), '0' ) >= 120 AND WORK_ORDER_STATE <> 4 THEN 1 WHEN ifnull( TIMESTAMPDIFF(    MINUTE,    WORK_ORDER_START_TIME,    ifnull(       WORK_ORDER_END_TIME,       SYSDATE()    ) ), '0' ) >= 15 AND WORK_ORDER_STATE = 2 THEN 1 ELSE 0 END ORDER1 FROM CM_FAULT_WORK_ORDER WHERE WORK_ORDER_ID = '456'";
  }
}

