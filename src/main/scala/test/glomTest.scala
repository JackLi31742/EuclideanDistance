package test

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object glomTest {
	val conf = new SparkConf().setMaster("spark://rtask-nod8:7077").setAppName("Euclidean-Distance")
			val sc = new SparkContext(conf)
  def glomTestDemo(){
    val a = sc.parallelize(List("wyp", "iteblog", "com", "397090770", "test"), 10)
    val b = sc.parallelize(List(3.0, 1.0, 9.0, 12.9, 4.9),10)
    val c = b.zip(a.zip(a))
//    var rdd = sc.makeRDD(1 to 100,10)
    var rddArr=c.glom().collect()
    println("partitions.size:"+c.partitions.size)
    println("--------------------------")
    for(i <- 0 until rddArr.length){
      var arr=rddArr(i)
      for(j <-0 until arr.length ){
        print(arr(j)+",")
      }
      println("--------------------------")
      
    }
//    rddArr.foreach(arr⇒arr.mkString(",").foreach(print))
  }
	
	//aggregate
  var rdd1 = sc.makeRDD(1 to 10,2)
rdd1.mapPartitionsWithIndex{
        (partIdx,iter) => {
          var part_map = scala.collection.mutable.Map[String,List[Int]]()
            while(iter.hasNext){
              var part_name = "part_" + partIdx;
              var elem = iter.next()
              if(part_map.contains(part_name)) {
                var elems = part_map(part_name)
                elems ::= elem
                part_map(part_name) = elems
              } else {
                part_map(part_name) = List[Int]{elem}
              }
            }
            part_map.iterator
           
        }
      }.collect
}