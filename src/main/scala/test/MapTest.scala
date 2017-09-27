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
  
  //不能这么做
  def doubleMap(){
	  val points= Array(Array(8.0,6.9,4.7), Array(3.0,5.0,2.9), Array(2.6,7.0,6.9), Array(4.3,2.0,9.9), Array(1.0,2.4,2.9))
    val points2 = Array(Array(9.0,3.9,4.7), Array(5.0,2.0,2.9), Array(3.6,5.0,6.9), Array(6.3,2.0,9.9), Array(2.0,2.4,2.9))
    val rddpoints=sc.parallelize(points).repartition(2)
     val rddpoints2=sc.parallelize(points2).repartition(2)
     println("rddpoints的partitions的大小是:"+rddpoints.partitions.size)
     println("rddpoints2的partitions的大小是:"+rddpoints2.partitions.size)
     val result=rddpoints.map(f1⇒{
       var buf2 = scala.collection.mutable.ArrayBuffer.empty[Double]
       rddpoints2.foreach(f2⇒{
        var distance = euclidean(f1,f2)
        buf2.+=:(distance)
       })
       buf2
     })
     println("result的partitions的大小是:"+result.partitions.size)
     result.collect().foreach(f⇒{
    	println("result:-------------------------------")
    	for(i <- 0 until f.length ){
          print(f(i)+",")
      }
      })
     
  }
  
  def euclidean(x: Array[Double], y: Array[Double]) :Double= {
//    val startTime = System.currentTimeMillis();
   var distance = 0.0;

		for (i <- 0 until x.length) {
			var temp = Math.pow((x(i) - y(i)), 2);
//			var temp = Math.pow(x(i), 2)+Math.pow(y(i), 2);
			distance += temp;
		}
//		val EndTime=System.currentTimeMillis();
//		println("Cost time of euclidean: " + (EndTime-startTime) + "ms")
		
		distance 
		/*distance = Math.sqrt(distance);
		return 1.0 / (1.0 + distance);*/

    
//   val d= math.sqrt(x.zip(y).map(p => p._1 - p._2).map(d => d * d).sum)
//   val d= math.sqrt(x.zip(y).map{case (x,y)=>(x-y)*(x-y)}.reduceLeft(_+_))
//   println("距离是："+d)
//   
//   val sim=1.0 / (1.0 + d)
//   println("相似度是："+sim)
//   sim
  }
}
