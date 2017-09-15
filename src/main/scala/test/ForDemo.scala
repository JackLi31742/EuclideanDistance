package test

import scala.math._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala._
import scala.collection.JavaConverters._
import java.util.Date
import org.apache.spark.rdd.RDD.rddToOrderedRDDFunctions
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import scala.collection.Seq

object ForDemo {
  val now = new Date()  
  def getCurrent_time():Long = {  
        val a = now.getTime  
        var str = a+""  
        
        str.toLong  
  }  
   def main(args:Array[String]){
     val start=getCurrent_time
    var x = Array(1.0f, 2.0f, 3.0f)
    		var y = Array(2.0f, 3.0f, 5.0f)
    		y.foreach(f⇒{
    		  println("==========")
    		  println(f)
    		})
    		val end =getCurrent_time
//    		euclidean(x, y)
    		println(start+"-"+end+"="+(start-end))
  }
  def a() {
    val conf = new SparkConf().setMaster("spark://rtask-nod8:7077").setAppName("Euclidean-Distance")
    val sc = new SparkContext(conf)
    case class Point(x: Double, y: Double)

    val points1 = List(("1", 3), ("2", 4), ("3", 5), ("4", 6))
    //  val rdd4=points1.toVector.groupBy(r⇒(r._1))
    val points2 = List(("4", 6),("1", 3), ("2", 4), ("3", 5))
    val rdd1 = sc.parallelize(points1)
    val rdd2 = sc.parallelize(points2)
    val rdd3 = rdd1.map(r ⇒ (r._1, r._2))
    val rdd4 = rdd1.map(r ⇒ (r._2, r._1))
    
      val rdd5=rdd1.zip(rdd2) 
      val rdd6=rdd5.map(r⇒(r._1._1,r._2._1,(r._1._2-r._2._2)))
    //  val rdd6=rdd3.zip(rdd3)
      rdd5.collect().foreach(println)
        println("5------------------")
      rdd6.collect().foreach(println)
    //  rdd6.foreach(println)
    //  println("6------------------")

  }
  //  points1.
  //  points1.toVector

  //  val pointsWithIndex = points1.zipWithIndex
  //
  //  def distance(a: Point, b: Point): Double = {
  //
  //    Math.sqrt((a.x - b.x) * (a.x - b.x) + (a.y - b.y) * (a.y - b.y))
  //  }
  //
  //  val dis = pointsWithIndex.flatMap(a => pointsWithIndex.filter(_._2 > a._2).map((a, _)))
  //  .map({ case (a, b) => (a, b, distance(a._1, b._1)) })
  //
  //  dis.foreach(println)

  def b() {
    val conf = new SparkConf().setMaster("spark://rtask-nod8:7077").setAppName("Euclidean-Distance")
    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(Seq(Seq(1, 2, 3), Seq(4, 5, 6), Seq(7, 8, 9)))
    rdd.collect().foreach(println)
    // Split the matrix into one number per line.
    val byColumnAndRow = rdd.zipWithIndex.flatMap {
      case (row, rowIndex) => row.zipWithIndex.map {
        case (number, columnIndex) => columnIndex -> (rowIndex, number)
      }
    }
    // Build up the transposed matrix. Group and sort by column index first.
    val byColumn = byColumnAndRow.groupByKey.sortByKey().values
    // Then sort by row index.
    val transposed = byColumn.map {
      indexedRow => indexedRow.toSeq.sortBy(_._1).map(_._2)
    }
    transposed.collect().foreach(println)
  }
}