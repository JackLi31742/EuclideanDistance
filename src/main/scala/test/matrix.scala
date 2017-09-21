package test

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{Vector,Vectors}
import org.apache.spark.mllib.linalg.distributed.IndexedRow
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix
import org.apache.spark.mllib.linalg.distributed.RowMatrix

class matrix {
  def testmatrix(){
    val conf = new SparkConf().setMaster("spark://rtask-nod8:7077").setAppName("Euclidean-Distance")
    val sc = new SparkContext(conf)
    val points1 = List(Array(("a", 9.0)), Array(("e", 7.0)), Array(("b", 5.6)), Array(("d", 6.3)))
    val points2 = Array(Array(9.0,3.9,4.7), Array(7.0,2.0,2.9), Array(5.6,5.0,6.9), Array(6.3,2.0,9.9))
    val points3 = Array(Array(9.0,3.9,4.7), Array(5.0,2.0,2.9), Array(3.6,5.0,6.9), Array(6.3,2.0,9.9))
    //repartition会改原来的顺序，但是生成的rdd和以后的操作的顺序是一样的
    val rdd=sc.parallelize(points2).repartition(2).zipWithIndex()
    val rddpoints3=sc.parallelize(points3).repartition(2).zipWithIndex()
    val rddre=rdd.cartesian(rddpoints3).foreach(f⇒{
      var posi:Long=0
      if(f._1._1.eq(f._2._1)){
       posi= f._1._2
      }
      posi
      })
    rdd.collect().foreach(f⇒{
      println("rdd:")
      println(f._1(0)+","+f._2)
      })
    val rdd1=rdd.map(f⇒(Vectors.dense(f._1),f._2))
        val rdd2=rdd1.map(f⇒new IndexedRow(f._2,f._1))
    val matrix=new IndexedRowMatrix(rdd2,4,3)
    println(matrix.rows.foreach(f⇒println(f)))
    println("行数："+matrix.numRows())
    println("列数："+matrix.numCols())
    
   val rdd3= rdd.mapPartitions(f⇒{
      f.map(f⇒(Vectors.dense(f._1),f._2)).map(f⇒new IndexedRow(f._2,f._1))
    })
    val rdd4=rdd3.mapPartitions(f⇒{
      f.map(f⇒f.vector.toArray)
      
      })
      val rdd5=rdd4.mapPartitions(f⇒{
        var result = List[Double]()
      var i = 0.0
         while(f.hasNext){
         i += f.next()(0)
         }
         result.::(i).iterator
      })
    rdd3.collect().foreach(f⇒{
      println("rdd3:")
      println(f)
      })
      rdd4.collect().foreach(f⇒{
      println("rdd4:")
      println(f(0))
      })
      println("rdd4的大小是："+rdd4.count())
      rdd5.collect().foreach(f⇒{
      println("rdd5:")
      println(f)
      })
     val matrix2=new IndexedRowMatrix(rdd2,4,3)
    println(matrix2.rows.foreach(f⇒{
      println("2:")
      println(f)
      }))
    println("行数2："+matrix2.numRows())
    println("列数2："+matrix2.numCols())
  }
}