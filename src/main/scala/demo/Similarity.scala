package demo
//import java.lang.Double
import java.lang.Math
import java.util.Date

import scala._
import scala.collection.JavaConverters._
import scala.collection.immutable.List

//import scala.math._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext._
import entities.ReIdAttributesTemp
import entities.ReIdAttributesTempRDD
import Similarity.EuclideanDistance.PedestrianReIDFeatureEuclideanDistanceSimilarityWithSpark._
import Similarity.EuclideanDistance.GraphDatabaseConnector
import Similarity.EuclideanDistance.PedestrianReIDFeatureEuclideanDistanceSimilarityWithSpark
import Similarity.EuclideanDistance._
import Similarity.EuclideanDistance.util.SingletonUtil
import Similarity.EuclideanDistance.Neo4jConnector
import Similarity.EuclideanDistance.util.Factory
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import java.util.ArrayList
import test.FloatPointerDemo._
import test.FloatPointerDemo

/*class ForDemo {
  case class Point(x: Double, y: Double)

  val points = List(Point(1, 1), Point(2, 2), Point(3, 3), Point(4, 4))

  val pointsWithIndex = points.zipWithIndex

  def distance(a: Point, b: Point): Double = {

    Math.sqrt((a.x - b.x) * (a.x - b.x) + (a.y - b.y) * (a.y - b.y))
  }

  val dis = pointsWithIndex.flatMap(a => pointsWithIndex.filter(_._2 > a._2).map((a, _)))
  .map({ case (a, b) => (a, b, distance(a._1, b._1)) })

  dis.foreach(println)
}*/

class Similarity extends Serializable{
  @transient
   val conf =new SparkConf().set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)
//		 .set("spark.driver.allowMultipleContexts","true")
   @transient
		val sc=new SparkContext(conf)
  val partition=10
////  @transient
//  val driver = GraphDatabase.driver("bolt://172.18.33.37:7687",
//            AuthTokens.basic("neo4j", "casia@1234"));
////  @transient
//	val session = driver.session();
//    @transient
//    val dbConnector=new Neo4jConnector();
      
  /*def combs(rdd:RDD[String]):RDD[(String,String)] = {
    val count = rdd.count
    if (rdd.count < 2) { 
        sc.makeRDD[(String,String)](Seq.empty)
    } else if (rdd.count == 2) {
        val values = rdd.collect
        sc.makeRDD[(String,String)](Seq((values(0), values(1))))
    } else {
        val elem = rdd.take(1)
        val elemRdd = sc.makeRDD(elem)
        val subtracted = rdd.subtract(elemRdd)  
        val comb = subtracted.map(e  => (elem(0),e))
        comb.union(combs(subtracted))
    } 
 }*/
  
  /* def similarity(list:List[ReIdAttributesTemp])={
    val conf =new SparkConf().setMaster("spark://rtask-nod8:7077").setAppName("Euclidean-Distance")
    val sc=new SparkContext(conf)
    println("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
    println("传入的list是："+list.toString())
    
    val pointsWithIndex=list.asScala.toList.zipWithIndex
    val dis = pointsWithIndex.flatMap(a => pointsWithIndex.filter(_._2 > a._2).map((a, _)))
  .map({ case (a, b) => (a._1.getTrackletID, b._1.getTrackletID, euclidean(a._1.getFeatureVector, b._1.getFeatureVector)) })
  println("结果是是：yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy")
  dis.foreach(println)

    
  }*/
  
	def reciveList(list:java.util.List[ReIdAttributesTemp]){
//	  val rdd=listToRdd(list)._1
//	  var sc  =listToRdd(list)._2
//	  glom(listToRdd(list)._1, listToRdd(list)._2)
	  
	}
	def listToVector(list:java.util.List[ReIdAttributesTemp]){
	  var v=list.asScala.toVector
	  v.foreach(println)
	}
  def listToRdd(list:java.util.List[ReIdAttributesTemp]
      ):(RDD[ReIdAttributesTemp])={
		
    println("传入的list大小为："+list.size())
    val rdd=sc.parallelize(list.asScala)
    println("partitions的大小是:"+rdd.partitions.size)
    return (rdd)
  }
  
  //glom and Broadcast
  def glom(rdd: RDD[ReIdAttributesTemp]
//      ,dbConnectorByte:Array[Byte]
//  ,dbConnSingleton:SingletonUtil[GraphDatabaseConnector]
  , args:Array[String]
  )={
    try{
//    rdd.repartition(10)
    //对于每个节点都得到KNN，目前没有高效的，可以避免重复计算的办法
//    var rddWithIndex=rdd.zipWithIndex()
    //直接使用rdd，不再再次赋给另一个rddWithIndex
//    		var rddWithIndex=rdd
    //这里原来是rddWithIndex
//    var rddGlom=rddWithIndex.repartition(10).glom()
    		var rddGlom=rdd.repartition(partition).glom()
    
//    println("checkpoint路径是:"+sc.getCheckpointDir)
//    rddGlom.map(f⇒{
//      var broadcastVar = sc.broadcast(f)
//      EuDis(rdd,broadcastVar)
//      broadcastVar.unpersist()
//    })
    var rddArr=rddGlom.collect()
    //而且由于每个都要得到KNN，所以就不需要汇总了
//    val resultListBuf = scala.collection.mutable.ListBuffer.empty[Array[(String, String, Double)]]
    var operationTime=0l 
    var dbTime=0l
    for(i <- 0 until rddArr.length){
      var arr=rddArr(i)
      val broadcastVar = sc.broadcast(arr)
      //rdd原来也是rddWithIndex
      val resultTime=everyOneNeedEuDis(rdd,broadcastVar, args)
      operationTime +=resultTime._1
      dbTime+=resultTime._2
     
//      resultListBuf +=result
      println(i+"次-----------------------------------结束")
//      list.+:(broadcastVar)
      broadcastVar.unpersist()
//      for(j <-0 until arr.length ){
//        print(arr(j).getTrackletID+",")
//      }
      
    }
    println("打印的最终结果是：")
    println("Cost time of operation: " + (operationTime) + "ms")
    println("Cost time of db: " + (dbTime) + "ms");
    }finally {
      println("glom finally");
//      dbConnSingleton.getInst.finalize()
//      rdd.unpersist()
//      sc.stop()
    }
    //.reverse
//     val resultListSort=resultListBuf.toList.flatMap(f⇒f).sortBy(f⇒(f._3))(Ordering.Double)
//    resultListSort.foreach(println)
    
//    resultListBuf.foreach(f⇒f.foreach(println))
//    return list
  }
  
  def printBroadcastList(list:List[Broadcast[Array[ReIdAttributesTemp]]]){
	  println("--------------------------")
	  list.foreach(f⇒f.value.foreach(f⇒println(f.getTrackletID)))
    
  }
  //笛卡尔积和Broadcast，每一个都要有KNN
  def everyOneNeedEuDis(rddWithIndex: RDD[ReIdAttributesTemp],broad:Broadcast[Array[ReIdAttributesTemp]]
//          ,dbConnectorByte:Array[Byte]  
//  ,dbConnSingleton:SingletonUtil[GraphDatabaseConnector]
  , args:Array[String]
  ) :(Long,Long)= {
//    val dbConnector=getConnector(dbConnectorByte)
//    rdd.collect().foreach{ReIdAttributesTemp⇒{
//      val fea=ReIdAttributesTemp.getFeatureVector
//      println(ReIdAttributesTemp.getTrackletID+","+fea(0))
//      }}
    
if(args(0).equals("minute")){
      
    
    val rdd1 = rddWithIndex.map(r ⇒ ((r.getTrackletID, r.getFeatureVector)))
    /*rdd1.collect().foreach(f⇒{
    	println("rdd1:-------------------------------")
      println(f)
      })*/
   /* var broadArr=broad.value
    for(i<- 0 until broadArr.length){
      
    }*/
    val broadRdd=sc.parallelize(broad.value).map(r ⇒ ((r.getTrackletID, r.getFeatureVector))).repartition(partition)
    /*broadRdd.collect().foreach(f⇒{
    	println("broadRdd:-------------------------------")
      println(f)
      })*/
    val caRDD = rdd1.cartesian(broadRdd)
    /*caRDD.collect().foreach(f⇒{
    	println("caRDD:-------------------------------")
      println(f)
      })*/
    val rdd2=caRDD.filter(r⇒r._1._1 != r._2._1).filter(r⇒(r._1._2!=null)&&(r._2._2!=null))
    
     /*println("rdd2的个数是："+rdd2.count())
    rdd2.collect().foreach(r⇒{
    	println("rdd2:-------------------------------")
      println(r)
      })*/
//    rdd2.collect().foreach(println)
//    val start=getCurrent_time
//    val rdd3=rdd2.map(r⇒(r._1._1,r._2._1,euclidean(r._1._2,r._2._2)))
    val rdd3=rdd2.map{case r⇒(r._2._1,(r._1._1,euclidean(r._1._2,r._2._2)))}
    /*val rdd3=rdd2.map{r⇒{
      
     var settings:FloatPointerDemo.Settings  = new FloatPointerDemo.Settings();
		var finder:FloatPointerDemo  = new FloatPointerDemo(settings);
		println(getMat(r._1._2))
		println(getMat(r._2._2))
		finder.flannFindPairs(getMat(r._1._2),getMat(r._2._2));
    }
      }*/
//    val rdd4=rdd3.first()
//    println("rdd4--------------------------------------:"+rdd4)
//    val end =getCurrent_time
//    println("欧氏距离计算用时:"+end+"-"+start+"="+(end-start))
//    c.sortByKey().collect.foreach(println)
//    val rdd4=rdd3.map(r⇒(r._3,r))
//    rdd4.collect().foreach(println)
//    println("传入的rdd4是：-------------------------------------------")
//    val re=rdd4.top(3)(Ordering.by[(Double, (String, String,Double)), Double](_._1))  
//    val startTop=getCurrent_time
//    
//    println("rdd3的个数是："+rdd3.count())
//    rdd3.collect().foreach(f⇒{
//    	println("rdd3:-------------------------------")
//      println(f)
//      })
    val b=rdd3.groupByKey()
    /*b.collect().foreach(f⇒{
    	println("b:-------------------------------")
      println(f)
      })*/
   val c= b.map(f⇒(f._1,(f._2.toList.sortBy(f⇒f._2))))
   /*c.collect().foreach(f⇒{
    	println("c:-------------------------------")
      println(f)
      })*/
      val TOP3startTime = System.currentTimeMillis();
    val result=c.map(f⇒(f._1,f._2.take(3)))
     val TOP3EndTime=System.currentTimeMillis();
    val result10=c.map(f⇒(f._1,f._2.take(10)))
    val TOP10EndTime=System.currentTimeMillis();
    println("Cost time of TOP3: " + (TOP3EndTime-TOP3startTime) + "ms")
    println("Cost time of TOP10: " + (TOP10EndTime-TOP3EndTime) + "ms")
//    val endTop =getCurrent_time
//    println("topk用时:"+endTop+"-"+startTop+"="+(endTop-startTop))
    println("topk 结束")
//    println("result的个数是："+result.count())
    val dbstartTime = System.currentTimeMillis();
//    val resultArr= result.collect()
    /*result.collect().foreach(f⇒{
    	println("result:-------------------------------")
      println(f)
      })*/
    val operationEndTime=System.currentTimeMillis();
    val operationEveryTime=operationEndTime - dbstartTime
    println("Cost time of every operation: " + (operationEveryTime) + "ms")

    //测试不收集会不会执行add操作
    //闭包，没用的
//    var i=0
    
    //依然是spark任务
    /*result.foreachPartition(f⇒{
    	var dbConnector:GraphDatabaseConnector=null;
    	try{
    	  
    		dbConnector=new Neo4jConnector();
    		println("dbConnector-"+i+":"+dbConnector.toString)
    	f.foreach(f⇒{
//      DbConnector.init()
//    	val dbConnector = DbConnector.getInstance()
    		val db=new Factory[Neo4jConnector](){
    			
    			def produce() :Neo4jConnector={
    					return new Neo4jConnector()
    			}
    			
    		};
    		val dbConnSingleton=new SingletonUtil[Neo4jConnector](db, classOf[Neo4jConnector]);
    		val dbConnector=dbConnSingleton.getInst()   
        for(i <- 0 until f._2.length){
            println("要保存的结果是：" +f._1+","+f._2(i)._1+","+f._2(i)._2)
            println("dbConnector的开启状态："+dbConnector.isOpen)
            dbConnector.addSimRel(f._1, f._2(i)._1, f._2(i)._2)
        }
//        dbConnector=null
        		i =i+ 1
        		println("i:"+i)
      })
    	}catch{
    	  case e: Exception => println("exception caught: " + e);
    	}finally {
//    		if(dbConnector!=null){
//    			dbConnector.finalize()
    		  dbConnector=null
//    		}
    	}
    	println("内层foreach结束---------------------------")
      
    })*/
    //foreach好像不再是spark任务
      result.foreachPartition(f⇒{
    	  
    	  var dbConnector:GraphDatabaseConnector=new Neo4jConnector();
    	f.foreach(f⇒{
    	  //循环的次数就是节点的个数
//    		println("dbConnector-"+i+":"+dbConnector.toString)
//      DbConnector.init()
//    	val dbConnector = DbConnector.getInstance()
    		/*val db=new Factory[Neo4jConnector](){
    			
    			def produce() :Neo4jConnector={
    					return new Neo4jConnector()
    			}
    			
    		};
    		val dbConnSingleton=new SingletonUtil[Neo4jConnector](db, classOf[Neo4jConnector]);
    		val dbConnector=dbConnSingleton.getInst()*/   
        for(i <- 0 until f._2.length){
            println("min需要保存的结果是：[{'sim':"+f._2(i)._2+",'trackletID1':'"+f._1+"','trackletID2':'"+f._2(i)._1+"'}]")
//            println("dbConnector的开启状态："+dbConnector.isOpen)
            if(!(f._2(i)._2.toString().equals("null"))){
              
            	val outlist=dbConnector.addSimRel(f._1, f._2(i)._1, f._2(i)._2)
            			println("min保存完成的结果是："+outlist.toString())
            }
        }
//        		i =i+ 1
      })
        		//如果不行再测试
        	dbConnector.finalize()

    	    dbConnector=null
//    	println("内层foreach结束---------------------------")
      
    })
//        		println("i:"+i)
//    println("外层foreachPartition结束---------------------------")
     val dbendTime = System.currentTimeMillis();
     val dbEveryTime=dbendTime - dbstartTime
     println("Cost time of erery db: " + (dbEveryTime) + "ms")
     println("min result:-------------------------------")
     return (operationEveryTime,dbEveryTime)
//     result
//    println("-----------------------------------------------------")
//    val rdd6=rdd4.sortByKey(false)
//    rdd6.collect().foreach(println)
//    println("传入的rdd6是：-------------------------------------------")
//    println("传入的rdd7是：-------------------------------------------")
//   var rdd7= rdd3.sortBy(r⇒(r._3),false)
//   println("-------------------------------------------------------------长度是:"+rdd7.count())
//   var rdd8=rdd7.collect()
//   rdd8.foreach(println)
//    val rdd5=rdd6.map(r⇒(r._2._1,r._2._2,r._1))
//    println("结果是：-------------------------------------------")
//    rdd5.collect().foreach(println)
//    (1,1)
     }

else if(args(0).equals("hour")){
      val rdd1 = rddWithIndex.map(r ⇒ ((r.getTrackletID, r.getFeatureVector,r.getStart)))
   /* rdd1.collect().foreach(f⇒{
    	println("rdd1:-------------------------------")
      println(f)
      })*/
   /* var broadArr=broad.value
    for(i<- 0 until broadArr.length){
      
    }*/
    val broadRdd=sc.parallelize(broad.value).map(r ⇒ ((r.getTrackletID, r.getFeatureVector,r.getStart))).repartition(partition)
   /* println("broadRdd的个数是："+broadRdd.count())
    broadRdd.collect().foreach(f⇒{
    	println("broadRdd:-------------------------------")
      println(f)
      })*/
    val caRDD = rdd1.cartesian(broadRdd)
    /*caRDD.collect().foreach(f⇒{
    	println("caRDD:-------------------------------")
      println(f)
      })*/
    val rdd2=caRDD.filter(r⇒r._1._1 != r._2._1).filter(r⇒(r._1._2!=null)&&(r._2._2!=null)&&(r._1._3!=r._2._3))
    
     /*println("rdd2的个数是："+rdd2.count())
    rdd2.collect().foreach(r⇒{
    	println("rdd2:-------------------------------")
      println(r)
      })*/
//    rdd2.collect().foreach(println)
//    val start=getCurrent_time
//    val rdd3=rdd2.map(r⇒(r._1._1,r._2._1,euclidean(r._1._2,r._2._2)))
    val rdd3=rdd2.map{case r⇒(r._2._1,(r._1._1,euclidean(r._1._2,r._2._2)))}
//    val rdd4=rdd3.first()
//    println("rdd4--------------------------------------:"+rdd4)
//    val end =getCurrent_time
//    println("欧氏距离计算用时:"+end+"-"+start+"="+(end-start))
//    c.sortByKey().collect.foreach(println)
//    val rdd4=rdd3.map(r⇒(r._3,r))
//    rdd4.collect().foreach(println)
//    println("传入的rdd4是：-------------------------------------------")
//    val re=rdd4.top(3)(Ordering.by[(Double, (String, String,Double)), Double](_._1))  
//    val startTop=getCurrent_time
    /*println("rdd3的个数是："+rdd3.count())
    rdd3.collect().foreach(f⇒{
    	println("rdd3:-------------------------------")
      println(f)
      })*/
    val b=rdd3.groupByKey()
    /*println("b的个数是："+b.count())
    b.collect().foreach(f⇒{
    	println("b:-------------------------------")
      println(f)
      })*/
    
     //必须收集才可以将work上的信息拿回来
    /*
    var list:java.util.List[ReIdAttributesTemp]=new ArrayList[ReIdAttributesTemp]()
    b.collect().foreach(f⇒{
    	var dbConnector:GraphDatabaseConnector=new Neo4jConnector();
    	  
    			val eachList=dbConnector.getPersonSimList(f._1)
//    					println("eachList:"+eachList.size()+", "+eachList.toString())
    					if(eachList!=null){
    						
    						list.addAll(eachList)
    					}
      dbConnector.finalize()
    	dbConnector=null
    })
    var rdd4: RDD[ReIdAttributesTemp]=null
    if(list!=null){
      
//    	println("list:"+list.toString())
    	rdd4=listToRdd(list)
    }
    rdd4.collect().foreach(f⇒{
    	println("rdd4:-------------------------------")
      println(f)
      })*/
      
//    var d:RDD[(String, (String, Double))]=null
    val e= b.map(f⇒{
      
    	var dbConnector:GraphDatabaseConnector=new Neo4jConnector();
    	  
    			val eachList=dbConnector.getPersonSimList(f._1)
//    					println("eachList:"+eachList.size()+", "+eachList.toString())
//    					if(eachList!=null){
//    					    var eachRdd=	listToRdd(eachList)
//    					    eachRdd.collect().foreach(f⇒{
//    	              println("eachRdd:-----------foreach内部--------------------")
//                    println(f)
//                    })
//    					    var eachOutRdd=eachRdd.map(f⇒(f.getTrackletID1,(f.getTrackletID2,f.getSim)))
//    					    d.++(eachOutRdd)
//    					    d.collect().foreach(f⇒{
//    	              println("d:-----------foreach内部--------------------")
//                    println(f)
//                    })
//    					}
      dbConnector.finalize()
    	dbConnector=null
      eachList
    })
   /*e.collect().foreach(f⇒{
    	for(i <- 0 until f.size()){
    		println("e:-------------------------------")
    	  println(f.get(i).toString())
    	}
      })*/
    
      val rdd6=e.flatMap(f⇒f.asScala).map(f⇒(f.getTrackletID1,(f.getTrackletID2,f.getSim)))
//        for(i <- 0 until f.size()){
////        (f.get(i).getTrackletID1,(f.get(i).getTrackletID2,f.get(i).getSim))
//          f.get(i)
//        }
        
      
      /*rdd6.collect().foreach(f⇒{
    	println("rdd6:-------------------------------")
      println(f)
      })*/
      //RDD[(String, Iterable[(String, Double)])]中的Iterable不能union
//    val rdd5=b.union(rdd6)
//    val rdd5=rdd3.union(rdd6)
    /*rdd5.collect().foreach(f⇒{
    	println("rdd5:-------------------------------")
      println(f)
      })*/
      //直接取top3是不对是，这样取出来的不是每一个节点的top3，而是这一次遍历的top3，而且这样，在后续插入数据库的时候，效率很低
//    val result=rdd5.top(3)(Ordering.by[(String, (String,Double)), Double](_._2._2).reverse)  
    
    val rdd5=rdd3.union(rdd6).groupByKey()
   val c= rdd5.map(f⇒(f._1,(f._2.toList.sortBy(f⇒f._2))))
   /*c.collect().foreach(f⇒{
    	println("c:-------------------------------")
      println(f)
      })*/
      val TOP3startTime = System.currentTimeMillis();
    val result=c.map(f⇒(f._1,f._2.take(3)))
     val TOP3EndTime=System.currentTimeMillis();
    val result10=c.map(f⇒(f._1,f._2.take(10)))
    val TOP10EndTime=System.currentTimeMillis();
    println("Cost time of TOP3: " + (TOP3EndTime-TOP3startTime) + "ms")
    println("Cost time of TOP10: " + (TOP10EndTime-TOP3EndTime) + "ms")
//    println("result的个数是："+result.count())
    
    println("topk 结束")
    
    val dbstartTime = System.currentTimeMillis();
//    val resultArr= result.collect()
    val operationEndTime=System.currentTimeMillis();
    val operationEveryTime=operationEndTime - dbstartTime
    println("Cost time of every operation: " + (operationEveryTime) + "ms")
    //如果直接使用rdd5.top(3),则result不再是rdd，所以直接foreach，未来看是否再使用sc变成rdd
    result.foreachPartition(f⇒{
      
    	var dbConnector=new Neo4jConnector();
    	f.foreach(f⇒{
//    		println("dbConnector:"+dbConnector.toString)
    		/*val db=new Factory[Neo4jConnector](){
    			
    			def produce() :Neo4jConnector={
    					return new Neo4jConnector()
    			}
    			
    		};
    		val dbConnSingleton=new SingletonUtil[Neo4jConnector](db, classOf[Neo4jConnector]);
    		val dbConnector=dbConnSingleton.getInst()
    */   
        for(i <- 0 until f._2.length){
            println("hour需要保存的结果是：[{'sim':"+f._2(i)._2+",'trackletID1':'"+f._1+"','trackletID2':'"+f._2(i)._1+"'}]")
            
             val outlist=dbConnector.addHourSimRel(f._1, f._2(i)._1, f._2(i)._2)
            println("hour保存完成的结果是："+outlist.toString())
        }
    		
      })
    	dbConnector.finalize()
    	dbConnector=null
    })
     val dbendTime = System.currentTimeMillis();
     val dbEveryTime=dbendTime - dbstartTime
     println("Cost time of erery db: " + (dbEveryTime) + "ms")
     println("result:-------------------------------")
     return (operationEveryTime,dbEveryTime)
    }else (1,1)
    }
  
  //笛卡尔积和Broadcast
   def EuDis(rddWithIndex: RDD[(ReIdAttributesTemp, Long)],broad:Broadcast[Array[(ReIdAttributesTemp, Long)]]) : Array[(String, String, Double)]= {
    
//    rdd.collect().foreach{ReIdAttributesTemp⇒{
//      val fea=ReIdAttributesTemp.getFeatureVector
//      println(ReIdAttributesTemp.getTrackletID+","+fea(0))
//      }}
    val rdd1 = rddWithIndex.map(r ⇒ ((r._1.getTrackletID, r._1.getFeatureVector),r._2))
    /*rdd1.collect().foreach(f⇒{
    	println("rdd1:-------------------------------")
      println(f)
      })*/
    val broadRdd=sc.parallelize(broad.value).map(r ⇒ ((r._1.getTrackletID, r._1.getFeatureVector),r._2)).repartition(5)
    /*broadRdd.collect().foreach(f⇒{
    	println("broadRdd:-------------------------------")
      println(f)
      })*/
    val caRDD = rdd1.cartesian(broadRdd)
    /*caRDD.collect().foreach(f⇒{
    	println("caRDD:-------------------------------")
      println(f)
      })*/
    val rdd2=caRDD.filter(r⇒r._1._1._1 != r._2._1._1).filter(r⇒r._1._2<r._2._2).filter(r⇒(r._1._1._2!=null)&&(r._2._1._2!=null))
//     println("rdd2的个数是："+rdd2.count())
    /*rdd2.collect().foreach(r⇒{
    	println("rdd2:-------------------------------")
      println(r._1._1._1,r._1._1._2(0),r._2._1._1,r._2._1._2(0))
      })*/
//    rdd2.collect().foreach(println)
//    val start=getCurrent_time
//    val rdd3=rdd2.map(r⇒(r._1._1,r._2._1,euclidean(r._1._2,r._2._2)))
    val rdd3=rdd2.map{case r⇒(r._1._1._1,r._2._1._1,euclidean(r._1._1._2,r._2._1._2))}
//    val rdd4=rdd3.first()
//    println("rdd4--------------------------------------:"+rdd4)
//    val end =getCurrent_time
//    println("欧氏距离计算用时:"+end+"-"+start+"="+(end-start))
//    c.sortByKey().collect.foreach(println)
//    val rdd4=rdd3.map(r⇒(r._3,r))
//    rdd4.collect().foreach(println)
//    println("传入的rdd4是：-------------------------------------------")
//    val re=rdd4.top(3)(Ordering.by[(Double, (String, String,Double)), Double](_._1))  
//    val startTop=getCurrent_time
    /*println("rdd3的个数是："+rdd3.count())
    rdd3.collect().foreach(f⇒{
    	println("rdd3:-------------------------------")
      println(f)
      })*/
    val result=rdd3.top(3)(Ordering.by[(String, String,Double), Double](_._3).reverse)  
//    val endTop =getCurrent_time
//    println("topk用时:"+endTop+"-"+startTop+"="+(endTop-startTop))
    println("topk 结束")
//     result.foreach(println)
     result
//    println("-----------------------------------------------------")
//    val rdd6=rdd4.sortByKey(false)
//    rdd6.collect().foreach(println)
//    println("传入的rdd6是：-------------------------------------------")
//    println("传入的rdd7是：-------------------------------------------")
//   var rdd7= rdd3.sortBy(r⇒(r._3),false)
//   println("-------------------------------------------------------------长度是:"+rdd7.count())
//   var rdd8=rdd7.collect()
//   rdd8.foreach(println)
//    val rdd5=rdd6.map(r⇒(r._2._1,r._2._2,r._1))
//    println("结果是：-------------------------------------------")
//    rdd5.collect().foreach(println)
    }
  
  //笛卡尔积
    def EuDis(rdd: RDD[ReIdAttributesTemp]) = {
    
//    rdd.collect().foreach{ReIdAttributesTemp⇒{
//      val fea=ReIdAttributesTemp.getFeatureVector
//      println(ReIdAttributesTemp.getTrackletID+","+fea(0))
//      }}
    
    val rdd1 = rdd.map(r ⇒ (r.getTrackletID, r.getFeatureVector)).zipWithIndex()
    val caRDD = rdd1.cartesian(rdd1)
//    caRDD.collect().foreach(println)
    val rdd2=caRDD.filter(r⇒r._1 != r._2).filter(r⇒r._1._2<r._2._2).filter(r⇒(r._1._1._2!=null)&&(r._2._1._2!=null))
//    rdd2.collect().foreach(println)
    val start=getCurrent_time
    val rdd3=rdd2.map(r⇒(r._1._1._1,r._2._1._1,euclidean(r._1._1._2,r._2._1._2)))
    val rdd4=rdd3.first()
    val end =getCurrent_time
    println("欧氏距离计算用时:"+end+"-"+start+"="+(end-start))
//    c.sortByKey().collect.foreach(println)
//    val rdd4=rdd3.map(r⇒(r._3,r))
//    rdd4.collect().foreach(println)
//    println("传入的rdd4是：-------------------------------------------")
//    val re=rdd4.top(3)(Ordering.by[(Double, (String, String,Double)), Double](_._1))  
    val startTop=getCurrent_time
    val re=rdd3.top(10)(Ordering.by[(String, String,Double), Double](_._3))  
    val endTop =getCurrent_time
    println("topk用时:"+endTop+"-"+startTop+"="+(endTop-startTop))
     re.foreach(println)
//    println("-----------------------------------------------------")
//    val rdd6=rdd4.sortByKey(false)
//    rdd6.collect().foreach(println)
//    println("传入的rdd6是：-------------------------------------------")
//    println("传入的rdd7是：-------------------------------------------")
   var rdd7= rdd3.sortBy(r⇒(r._3),false)
//   println("-------------------------------------------------------------长度是:"+rdd7.count())
//   var rdd8=rdd7.collect()
//   rdd8.foreach(println)
//    val rdd5=rdd6.map(r⇒(r._2._1,r._2._2,r._1))
//    println("结果是：-------------------------------------------")
//    rdd5.collect().foreach(println)
    }
  
  //从java双层循环中将矩阵带出来
  def EuDis2(rdd: RDD[ReIdAttributesTempRDD]) = {
    println("yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy")
    
    println("传入的rdd是：-------------------------------------------")
    rdd.collect().foreach(println)
    
//    print(rdd.collect().mkString(","))
//    rdd.collect().foreach(println)
    val rdd1 = rdd.map(r ⇒ (r.getX,r.getY,r.getId1,r.getReIdAttributesTemp1.getFeatureVector,r.getId2, r.getReIdAttributesTemp2.getFeatureVector))
    rdd1.collect().foreach(println)
    println("打印的rdd1是：-------------------------------------------")
    val rdd15 =rdd1.map(r⇒ (euclidean(r._4,r._6),(r._3,r._5)))
//    val rdd18 =rdd15.map(r⇒(r.toVector).apply(0))
//    val rdd19 =rdd18.map(r⇒(r.getTrackletID, r.getFeatureVector))
////    val rdd16 =rdd15.map(r⇒(sc.parallelize(r)))
////    val rdd17 =rdd16.map(r⇒(r.map(r⇒(r.getTrackletID))))
//    val rdd21 =rdd1 join rdd19
     rdd15.collect().foreach(println)
    println("打印的rdd15是：-------------------------------------------")
//    val rdd11= rdd1.zipWithIndex()
//    for{
//	(a,i)<-rdd11
//	(b,j)<-rdd11 if j>i
//}yield((a,i),(b,j),euclidean(a._1._2,b._1._2))
//   val rdd13 = rdd1.zipWithIndex.map(p => (p._1._1, p._1._2, p._2))
//  for {
//    (id1, v1, i) <- rdd1
//    (id2, v2, j) <- rdd1 if j > i
//   } yield (id1, id2, euclidean(v1, v2))

//    val rdd12= rdd11.flatMap(r  ⇒   rdd11.filter(_._2 >r._2).map((r,_)))
//    rdd12.map({case (a, b) => (a, b, euclidean(a._1, b._1))})
    
//    val rdd2 = rdd1.map(r ⇒ (r._1, (r._1, r._2)))
//    val rdd3 = rdd2 join rdd2
////    val rdd3 = rdd1.cogroup(rdd1)
//    println("传入的rdd3是：----------------------------------------------------------")
////    rdd3.map{ case ((a, arr1),(i,j)) => ((a, arr1(0)),(i,j)) }.foreach(println)
////    rdd3.collect().foreach(r⇒println{((r._1._1,r._1._2(0)),(r._2._1,r._2._2))})
//    println("打印的rdd3是：-------------------------------------------")
//        rdd3.collect().foreach(println)
//        println(s"Number of entries in RDD is ${rdd3.count()}")
////        val rdd4=rdd1.zip(rdd1)
////        rdd4.collect().foreach(println)
////    val rdd4 = rdd3.map(r ⇒ ((r._2._1._1, r._2._2._1), (r._2._1._2, r._2._2._2)))
//    println("传入的rdd4是：----------------------------------------------------------")
////    rdd4.collect().foreach(println)
////    //    val rdd5 = rdd4.map(r ⇒ (r._1, (r._2._1 - r._2._2) * (r._2._1 - r._2._2))).reduceByKey(_ + _)
////    val rdd5 = rdd4.map(r ⇒ (r._1, euclidean(r._2._1, r._2._2))).reduceByKey(_ + _)
////    println("传入的rdd5是：----------------------------------------------------------")
////    rdd5.collect().foreach(println)
////    val rdd6 = rdd4.map(r ⇒ (r._1, 1)).reduceByKey(_ + _)
////    println("传入的rdd6是：----------------------------------------------------------")
////    rdd6.collect().foreach(println)
////    val rdd7 = rdd5.filter(r ⇒ r._1._1 != r._1._2)
////    println("传入的rdd7是：----------------------------------------------------------")
////    rdd7.collect().foreach(println)
////    val rdd8 = rdd7.join(rdd6)
////    println("传入的rdd8是：----------------------------------------------------------")
////    rdd8.collect().foreach(println)
////    val rdd9 = rdd8.map(r ⇒ (r._1._1, r._1._2, r._2._2 / (1 + sqrt(r._2._1))))
////    println("传入的rdd9是：----------------------------------------------------------")
////    rdd9.collect().foreach(println)
////    val rdd10=rdd9.map(r ⇒ Simi(r._1, r._2, r._3))
////    println("传入的rdd10是：----------------------------------------------------------")
////    rdd10.collect().foreach(println)
////    rdd10
  }
  
  def EuDis1(rdd: RDD[ReIdAttributesTemp]) = {
    println("yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy")
    
    println("传入的rdd是：-------------------------------------------")
    rdd.collect().foreach{ReIdAttributesTemp⇒{
      val fea=ReIdAttributesTemp.getFeatureVector
      println(ReIdAttributesTemp.getTrackletID+","+fea(0))
      }}
    
//    print(rdd.collect().mkString(","))
//    rdd.collect().foreach(println)
    val rdd1 = rdd.map(r ⇒ (r.getTrackletID, r.getFeatureVector))
    val rdd15 =rdd.map(r⇒ (scala.collection.immutable.List(r)))
    val rdd18 =rdd15.map(r⇒(r.toVector).apply(0))
    val rdd19 =rdd18.map(r⇒(r.getTrackletID, r.getFeatureVector))
//    val rdd16 =rdd15.map(r⇒(sc.parallelize(r)))
//    val rdd17 =rdd16.map(r⇒(r.map(r⇒(r.getTrackletID))))
    val rdd21 =rdd1 join rdd19
    rdd21.collect().foreach(println)
    println("打印的rdd21是：-------------------------------------------")
//    val rdd11= rdd1.zipWithIndex()
//    for{
//	(a,i)<-rdd11
//	(b,j)<-rdd11 if j>i
//}yield((a,i),(b,j),euclidean(a._1._2,b._1._2))
//   val rdd13 = rdd1.zipWithIndex.map(p => (p._1._1, p._1._2, p._2))
//  for {
//    (id1, v1, i) <- rdd1
//    (id2, v2, j) <- rdd1 if j > i
//   } yield (id1, id2, euclidean(v1, v2))

//    val rdd12= rdd11.flatMap(r  ⇒   rdd11.filter(_._2 >r._2).map((r,_)))
//    rdd12.map({case (a, b) => (a, b, euclidean(a._1, b._1))})
    
    val rdd2 = rdd1.map(r ⇒ (r._1, (r._1, r._2)))
    val rdd3 = rdd2 join rdd2
//    val rdd3 = rdd1.cogroup(rdd1)
    println("传入的rdd3是：----------------------------------------------------------")
//    rdd3.map{ case ((a, arr1),(i,j)) => ((a, arr1(0)),(i,j)) }.foreach(println)
//    rdd3.collect().foreach(r⇒println{((r._1._1,r._1._2(0)),(r._2._1,r._2._2))})
    println("打印的rdd3是：-------------------------------------------")
        rdd3.collect().foreach(println)
        println(s"Number of entries in RDD is ${rdd3.count()}")
//        val rdd4=rdd1.zip(rdd1)
//        rdd4.collect().foreach(println)
//    val rdd4 = rdd3.map(r ⇒ ((r._2._1._1, r._2._2._1), (r._2._1._2, r._2._2._2)))
    println("传入的rdd4是：----------------------------------------------------------")
//    rdd4.collect().foreach(println)
//    //    val rdd5 = rdd4.map(r ⇒ (r._1, (r._2._1 - r._2._2) * (r._2._1 - r._2._2))).reduceByKey(_ + _)
//    val rdd5 = rdd4.map(r ⇒ (r._1, euclidean(r._2._1, r._2._2))).reduceByKey(_ + _)
//    println("传入的rdd5是：----------------------------------------------------------")
//    rdd5.collect().foreach(println)
//    val rdd6 = rdd4.map(r ⇒ (r._1, 1)).reduceByKey(_ + _)
//    println("传入的rdd6是：----------------------------------------------------------")
//    rdd6.collect().foreach(println)
//    val rdd7 = rdd5.filter(r ⇒ r._1._1 != r._1._2)
//    println("传入的rdd7是：----------------------------------------------------------")
//    rdd7.collect().foreach(println)
//    val rdd8 = rdd7.join(rdd6)
//    println("传入的rdd8是：----------------------------------------------------------")
//    rdd8.collect().foreach(println)
//    val rdd9 = rdd8.map(r ⇒ (r._1._1, r._1._2, r._2._2 / (1 + sqrt(r._2._1))))
//    println("传入的rdd9是：----------------------------------------------------------")
//    rdd9.collect().foreach(println)
//    val rdd10=rdd9.map(r ⇒ Simi(r._1, r._2, r._3))
//    println("传入的rdd10是：----------------------------------------------------------")
//    rdd10.collect().foreach(println)
//    rdd10
  }

  
  //欧氏距离
  def euclidean(x: Array[Float], y: Array[Float]) :Double= {
//    println("eeeeeeeeeeee=========================eeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")
//   println("传入的x是："+x(0)) 
//   println("传入的y是："+y(0)) 
   var distance = 0.0;

		for (i <- 0 until x.length) {
			var temp = Math.pow((x(i) - y(i)), 2);
			distance += temp;
		}
		
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
  
  def foreachPrint(rdd:RDD[Simi]){
    rdd.collect().foreach{Simi⇒{
      print("---------------------scala test-------------" )
      print(Simi.id1+","+Simi.id2+","+Simi.similar)
      }
    }
    
  }
  
  def main(args:Array[String]){
    var x = Array(1.0f, 2.0f, 3.0f)
    		var y = Array(2.0f, 3.0f, 5.0f)
//    		euclidean(x, y)
    		
  }
  
  val now = new Date()  
  def getCurrent_time():Long = {  
        val a = now.getTime  
        var str = a+""  
        
        str.toLong  
  }
}


case class Simi(
  val id1: String,
  val id2: String,
  val similar: Double) extends Serializable

case class Item(
  val uid: String,
  val iid: String,
  val pref: Double) extends Serializable
    
//单例模式    
object DbConnector{
  var instance:GraphDatabaseConnector = new Neo4jConnector();
  def getInstance():GraphDatabaseConnector = {
    return instance
  }
  def init(){
    instance = new Neo4jConnector()
  }
}
class DbConnector private() extends  Serializable{
}



    