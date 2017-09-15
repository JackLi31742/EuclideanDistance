package test
import org.apache.spark.mllib.linalg._
import scala.math._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala._
import scala.collection.JavaConverters._
import org.apache.spark.mllib.linalg.distributed._
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import scala.collection.Seq
object vector {

  def car() {
    val conf = new SparkConf().setMaster("spark://rtask-nod8:7077").setAppName("Euclidean-Distance")
    val sc = new SparkContext(conf)
    val a = sc.parallelize(List((1, 1), (2, 2), (3, 3),(4,4))).zipWithIndex()
    val caRDD = a.cartesian(a)
    ////    val rdd7 = rdd5.filter(r ⇒ r._1._1 != r._1._2)
    caRDD.collect().foreach(println)
    val rdd2=caRDD.filter(r⇒r._1 != r._2).filter(r⇒r._1._2<r._2._2)
//    rdd2.filter(r⇒r)
    rdd2.collect().foreach(println)
//    val rdd=caRDD.map(x => (x._1, x._2, math.sqrt(math.pow((x._1._1 - x._2._1), 2) + math.pow((x._1._2 - x._2._2), 2)))).collect
//    rdd.foreach(println)
  }
  def test() = {
    var arr = Array(1.0, 2, 3, 4)
    var dm = new DenseMatrix(1, 4, arr)
    var dmt = dm.transpose
    var ma = dm.multiply(dmt)
    var dv: DenseVector = new DenseVector(arr)
    println(dm.toString())
    println(dmt.toString())
    //    println(dm.toB)
    //    var vec=Vectors.dense(arr)
    //    arr.map(_*10).foreach(println)
    //    println("a")
    val conf = new SparkConf().setMaster("spark://rtask-nod8:7077").setAppName("Euclidean-Distance")
    val sc = new SparkContext(conf)
    val rdd = sc.makeRDD(Seq(Vectors.dense(arr)))
    val mat: RowMatrix = new RowMatrix(rdd)

    println("-------------------------------------------------------")
    //   println(mat.multiply(dmt).) 
    println("-------------------------------------------------------")
    val m = mat.numRows()
    val n = mat.numCols()
    //    println(mat.toString())
    //    rdd.collect().foreach(println)
    for (i <- 0 until 1) {
      for (j <- 0 until 4) {
        println("i:" + i + ",j:" + j + ",元素是：" + ma.apply(i, j))
      }
    }
  }
  def main(args: Array[String]) {
    //    var arr=Array(1.0,2,3,4)
    //   var dm=new DenseMatrix(4, 1, arr)
    //    var dmt=dm.transpose
    //    var ma=dm.multiply(dmt)
    //    for(i <- 0 until 4){
    //      for(j <- 0 until 4){
    //        println("i:"+i+",j:"+j+",元素是："+ma.apply(i, j))
    //      }
    //    }

  }
}