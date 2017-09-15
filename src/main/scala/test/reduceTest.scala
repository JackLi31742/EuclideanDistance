package test

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.hadoop.fs.LocalFileSystem
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

class reduceTest {
  def reducetest(){
    @transient
   val conf =new SparkConf().setAppName("Euclidean-Distance").set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)
//		 .set("spark.driver.allowMultipleContexts","true")
   @transient
		 var sc=new SparkContext(conf)
    
    val a = sc.parallelize(List(("a",("2a",3)),("b",("4d",7)),("c",("5d",6)),("a",("3d",2)),("c",("4d",1))))
    val b=a.groupByKey()
   val c= b.map(f⇒(f._1,(f._2.toList.sortBy(f⇒f._2))))
   c.collect().foreach(println)
  }
}