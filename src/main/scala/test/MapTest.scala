package test

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import java.util.Arrays

object MapTest {
  val conf = new SparkConf().setMaster("spark://rtask-nod8:7077").setAppName("Euclidean-Distance")
  val sc = new SparkContext(conf)
  def map() {
    // TODO Auto-generated method stub
    var rdd1 = sc.makeRDD(1 to 5, 2)
    var rdd3 = rdd1.mapPartitions { x =>
      {
        var result = List[Int]()
        var i = 0
        while (x.hasNext) {
          i += x.next()
        }
        result.::(i).iterator
      }
    }
  }
  
  def MapPartitionsTest(){
    val points3 = Array(Array(9.0,3.9,4.7), Array(5.0,2.0,2.9), Array(3.6,5.0,6.9), Array(6.3,2.0,9.9), Array(2.0,2.4,2.9))
     val rddpoints3=sc.parallelize(points3).repartition(2)
     val rdd3= rddpoints3.mapPartitionsWithIndex((x,f)⇒{
       var buf1 = scala.collection.mutable.ArrayBuffer.empty[Double]
       var result = scala.collection.mutable.ArrayBuffer.empty[Array[Double]]
//        f.map(f⇒{
      while(f.hasNext){
        	buf1.appendAll(f.next())
      }
//        })
        val arr=buf1.toArray
        arr.toIterator
       result.+=(arr).iterator
//        result.::(arr)
    })
    rdd3.collect.foreach(f⇒{
        println("rdd3---------------------")
        println(Arrays.toString(f))
        })
    /*val rdd5=rdd3.mapPartitions(f⇒{
    var result = List[Double]()
      var i = 0.0
         while(f.hasNext){
         i += f.next()
         }
         result.::(i).iterator
      })
      rdd5.collect().foreach(f⇒{
      println("rdd5:")
      println(f)
      })*/
  }
}
