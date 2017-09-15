package test
import org.apache.spark.mllib.linalg._
import scala.math._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.JavaConverters._
import org.apache.spark.mllib.linalg.distributed._
import org.apache.spark.rdd.RDD.rddToOrderedRDDFunctions
object sortTest {
  def sort() {
    val conf = new SparkConf().setMaster("spark://rtask-nod8:7077").setAppName("Euclidean-Distance")
    val sc = new SparkContext(conf)
    val a = sc.parallelize(List("wyp", "iteblog", "com", "397090770", "test"), 2)
    val b = sc.parallelize(List(3.0, 1.0, 9.0, 12.9, 4.9))
    val c = b.zip(a.zip(a))
    val re=c.top(3)(Ordering.by[(Double, (String, String)), Double](_._1))  
    re.foreach(println)
    println("-----------------------------------------------------")
//    c.sortByKey().collect.foreach(println)
    implicit val sortIntegersByString = new Ordering[Int] {
      override def compare(a: Int, b: Int) =
        a.toString.compare(b.toString)
    }
    c.sortByKey().collect.foreach(println)
  }
}