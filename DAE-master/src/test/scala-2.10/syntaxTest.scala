import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.sql.Row
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import org.apache.spark.mllib.linalg.{Vectors, Vector}

/**
  * Created by asus on 2016/12/15.
  */
object syntaxTest {

  def main(args: Array[String]) {
     val a = Set(0,1)
    val b = Set(0,1)
    println(a.equals(b))

    //    val c1 = new Print1 {val}
    //    c1.show()
    //implicit lazy val formats = Serialization.formats(NoTypeHints)
    //        val a = "{\"a\":0,\"b\":1}"
    //        case class Vo(a:Boolean,b:Boolean)
    //   println(Serialization.write(Vo(true,false)))

    //        val Vo(c,d) =  Serialization.read[Vo](a)
    //        println(c)
    //        println(d)
  }

  def throwException(): Unit = {
    try {
      3 / 0
    } catch {
      case ex: Exception => throw new Exception("dfdsafas", ex)
    }
  }
}
