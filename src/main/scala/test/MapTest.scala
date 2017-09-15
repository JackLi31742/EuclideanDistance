package test

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

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
}
