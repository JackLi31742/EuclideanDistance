package demo
import java.nio.FloatBuffer
import java.nio.IntBuffer
import java.util.Date

import scala.collection.JavaConverters.asScalaBufferConverter

//import scala.math._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.bytedeco.javacpp.opencv_core.Mat

import Similarity.EuclideanDistance.GraphDatabaseConnector
import Similarity.EuclideanDistance.JavaKnn
import Similarity.EuclideanDistance.Neo4jConnector
import entities.ReIdAttributesTemp
import entities.ReIdAttributesTempRDD
import java.util.ArrayList
import java.util.Arrays
import java.util.Collection

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

class Similarity2 extends Serializable{
  @transient
   val conf =new SparkConf().set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)
                             .set("spark.executor.heartbeatInterval", "100000")
                             .set("spark.network.timeout", "10000000")
//                             .set("spark.shuffle.blockTransferService", "nio")
//		 .set("spark.driver.allowMultipleContexts","true")
   @transient
		val sc=new SparkContext(conf)
  val partition=3
  val col=128
  val minK=3
  val hourK=10
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
    println("rdd的partitions的大小是:"+rdd.partitions.size)
    return (rdd)
  }
  
  //使用flann
  def glomWithFlann(rdd: RDD[ReIdAttributesTemp]
  , args:Array[String]
  )={
    try{
//    rdd.repartition(10)
    //对于每个节点都得到KNN，目前没有高效的，可以避免重复计算的办法
//    var rddWithIndex=rdd.zipWithIndex()
    //直接使用rdd，不再再次赋给另一个rddWithIndex
//    		var rddWithIndex=rdd
    //这里原来是rddWithIndex
    		//小数据量需要
//      var rdd=rdd.repartition(partition)
    var rddGlom=rdd.glom()
    rdd.persist(StorageLevel.MEMORY_AND_DISK)
    println("rddGlom的partitions的大小是:"+rddGlom.partitions.size)
//    		var rddGlom=rdd.glom()
    
//    println("checkpoint路径是:"+sc.getCheckpointDir)
//    rddGlom.map(f⇒{
//      var broadcastVar = sc.broadcast(f)
//      EuDis(rdd,broadcastVar)
//      broadcastVar.unpersist()
//    })
    		
//    				var operationTime=0l 
//    				var dbTime=0l
//    				val count=sc.accumulator(0)
    				
    //而且由于每个都要得到KNN，所以就不需要汇总了
//    val resultListBuf = scala.collection.mutable.ListBuffer.empty[Array[(String, String, Double)]]
    
    var rddArr=rddGlom.collect()
    for(i <- 0 until rddArr.length){
      var arr=rddArr(i)
      if(arr.length>0){
          val broadcastVar = sc.broadcast(arr)
//          var broadcastRdd=sc.parallelize(broadcastVar.value)
//          println("broadcastRdd的partitions的大小是:"+broadcastRdd.partitions.size)
          everyOneNeedEuDisWithFlann(rdd, broadcastVar, args)

          println(i + "次-----------------------------------结束")
          broadcastVar.unpersist()
//          broadcastRdd.unpersist()
      }
    }
    		
//    		rdd.unpersist()
//    		rddGlom.unpersist()
    println("打印的最终结果是：")

    }finally {
    rdd.unpersist()
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
  
  //glom and Broadcast
  def glom(rdd: RDD[ReIdAttributesTemp]
//      ,dbConnectorByte:Array[Byte]
//  ,dbConnSingleton:SingletonUtil[GraphDatabaseConnector]
  , args:Array[String]
  )={
    try{
//    rdd.repartition(10)
    //对于每个节点都得到KNN，目前没有高效的，可以避免重复计算的办法
    var rddWithIndex=rdd.zipWithIndex()
    //直接使用rdd，不再再次赋给另一个rddWithIndex
//    		var rddWithIndex=rdd
    //这里原来是rddWithIndex  .repartition(partition)
    var rddGlom=rddWithIndex.glom()
      //小数据量需要.repartition(partition)
//    		var rddGlom=rdd.glom()
    		 
    
//    println("checkpoint路径是:"+sc.getCheckpointDir)
//    rddGlom.map(f⇒{
//      var broadcastVar = sc.broadcast(f)
//      EuDis(rdd,broadcastVar)
//      broadcastVar.unpersist()
//    })
    		
//    				var operationTime=0l 
//    				var dbTime=0l
//    				val count=sc.accumulator(0)
    				
    val resultListBuf = scala.collection.mutable.ListBuffer.empty[ RDD[(String, String, Double)]]
    
    var rddArr=rddGlom.collect()
    for(i <- 0 until rddArr.length){
      var arr=rddArr(i)
      if(arr.length>0){
          val broadcastVar = sc.broadcast(arr)
          
      //rdd原来也是rddWithIndex
      
          val result=everyOneNeedEuDis(rddWithIndex,broadcastVar, args)
//      operationTime +=resultTime._1
//      dbTime+=resultTime._2
     
      resultListBuf +=result
          println(i+"次-----------------------------------结束")
//      list.+:(broadcastVar)
          broadcastVar.unpersist()
//      for(j <-0 until arr.length ){
//        print(arr(j).getTrackletID+",")
//      }
      }
    }
    var unionRdd:RDD[(String, String, Double)]=resultListBuf(0)
    for(i <- 1 until resultListBuf.length){
      unionRdd=	unionRdd.union(resultListBuf(i))
      
    }
    /*println("unionRdd的个数是："+unionRdd.count())
    unionRdd.collect().foreach(f⇒{
    	println("unionRdd:-------------------------------")
      println(f)
      })*/
    val allRdd=unionRdd.union(unionRdd.map(f⇒(f._2,f._1,f._3)))
    /*println("allRdd的个数是："+allRdd.count())
    allRdd.collect().foreach(f⇒{
    	println("allRdd:-------------------------------")
      println(f)
      })*/
    val AllMaprdd=allRdd.map(f⇒(f._1,(f._2,f._3)))
    /*AllMaprdd.collect().foreach(f⇒{
    	println("AllMaprdd:-------------------------------")
      println(f)
      })*/
    val b=AllMaprdd.groupByKey()
    /*b.collect().foreach(f⇒{
    	println("b:-------------------------------")
      println(f)
      })*/
   val c= b.map(f⇒(f._1,(f._2.toList.sortBy(f⇒f._2))))
   /*c.collect().foreach(f⇒{
    	println("c:-------------------------------")
      println(f)
      })*/
   
   //计算top的时间
    val result=c.map(f⇒(f._1,f._2.take(3)))
    
    
    println("topk 结束")
      result.foreachPartition(f ⇒ {

        var dbConnector: GraphDatabaseConnector = new Neo4jConnector();
        f.foreach(f ⇒ {
          for (i <- 0 until f._2.length) {
            println("min需要保存的结果是：[{'sim':" + f._2(i)._2 + ",'trackletID1':'" + f._1 + "','trackletID2':'" + f._2(i)._1 + "'}]")
            //            println("dbConnector的开启状态："+dbConnector.isOpen)
            if (!(f._2(i)._2.toString().equals("null"))) {

              val outlist = dbConnector.addSimRel(f._1, f._2(i)._1, f._2(i)._2,args(1))
              println("min保存完成的结果是：" + outlist.toString())
            }
          }
        })
        	dbConnector.release()

    	    dbConnector=null
//    	println("内层foreach结束---------------------------")
      
    })
    result.unpersist()
    		/*
    		 * 测试失败
    		rddGlom.foreach(f⇒{
//    		  while(f.hasNext){
    		    var temp=f
    		    for(j <-0 until temp.length){
    		      
    		    	println("temp:"+count+":"+temp(j).getTrackletID)
    		    }
//    		  f.foreach(f⇒{
    		    val broadcastVar = sc.broadcast(temp)
    		    val resultTime=everyOneNeedEuDis(rdd,broadcastVar, args)
    		    operationTime +=resultTime._1
            dbTime+=resultTime._2
            count+=1
            broadcastVar.unpersist()
            println(count+"次----------------while-------------------结束")
//    		  })
//    		}
    		  println(count+"次---------------foreachPartition-------------------结束")
    		})
    		println(count+"次----------------out-------------------结束")
    		*/
    		
    println("打印的最终结果是：")
//    println("Cost time of operation: " + (operationTime) + "ms")
//    println("Cost time of db: " + (dbTime) + "ms");
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
  def everyOneNeedEuDisWithFlann(rdd: RDD[ReIdAttributesTemp],broad:Broadcast[Array[ReIdAttributesTemp]]
//          ,dbConnectorByte:Array[Byte]  
//  ,dbConnSingleton:SingletonUtil[GraphDatabaseConnector]
  , args:Array[String]
  )  {
//    val dbConnector=getConnector(dbConnectorByte)
//    rdd.collect().foreach{ReIdAttributesTemp⇒{
//      val fea=ReIdAttributesTemp.getFeatureVector
//      println(ReIdAttributesTemp.getTrackletID+","+fea(0))
//      }}
    
if(args(0).equals("minute")){
  
    rdd.collect().foreach(f ⇒ {
        println("rddWithIndex:-------------------------------")
        println(f)
    })
    val rdd1 = rdd.map(r ⇒(r.getTrackletID,r.getFeatureVector))
    
    println("rdd1的partitions的大小是:"+rdd1.partitions.size)
    println("rdd1的大小是：" + rdd1.count())
    rdd1.collect().foreach(f⇒{
    	println("rdd1:-------------------------------")
//    	for(i <- 0 until f.length ){
//          print(f(i)+",")
//      }
    	println(f._1+":"+f._2(0))
      })
     /* var rddGlom=rdd1.glom()
      rddGlom.collect().foreach(f⇒{
    	println("rddGlom:-------------------------------")
    	for(i <- 0 until f.length){
      println(f(i)._1+":"+f(i)._2)
    	}
      })*/
    /* val rdd1List= rdd1.map(f⇒f.toList)
     
     val rdd1ListReduce=rdd1List.reduce(List.concat(_,_))
     rdd1ListReduce.foreach(f⇒{
    	println("rdd1ListReduce:-------------------------------")
      println(f)
      })
      val rdd1Array=rdd1ListReduce.toArray
      println("rdd1Array的大小是:"+rdd1Array.length)
      for(i <- 0 until rdd1Array.length){
        println("rdd1Array:-------------------------------")
        println(rdd1Array(i))
      }
      val rdd1ArrayRdd=sc.parallelize(rdd1Array)
      */
      /*
      val rdd1ParList=rdd1.mapPartitions(f⇒{
       val v= f.map(f⇒f.toList)
       v.foreach(f⇒{
         println("rdd1ParList:-------------------------------")
         println(f)
         })
         println("---------分割线---------")
       v
      })
//      rdd1ParList.collect().foreach(f⇒{
//    	println("rdd1ParList:-------------------------------")
//      println(f)
//      })
//     val c= rdd1ParList.flatMap(f⇒f.:::(f))
     //返回到了driver端
      val rdd1ListReduce=rdd1ParList.reduce(List.concat(_,_))
     rdd1ListReduce.foreach(f⇒{
    	println("rdd1ListReduce:-------------------------------")
      println(f)
      })
      val rdd1Array=rdd1ListReduce.toArray
      println("rdd1Array的大小是:"+rdd1Array.length)
      for(i <- 0 until rdd1Array.length){
        println("rdd1Array:-------------------------------")
        println(rdd1Array(i))
      }*/
//      val rdd1ArrayRdd=sc.parallelize(rdd1Array)
      
   /* var broadArr=broad.value
    for(i<- 0 until broadArr.length){
      
    }*/
      //.repartition(partition)
    val broadRdd=sc.parallelize(broad.value).map(r ⇒ (r.getTrackletID,r.getFeatureVector))
    println("broadRdd的partitions的大小是:"+broadRdd.partitions.size)
    println("broadRdd的大小是：" + broadRdd.count())
    broadRdd.collect().foreach(f⇒{
    	println("broadRdd:-------------------------------")
//      for(i <- 0 until f._2.length ){
//          print(f._1+":"+f._2(i)+",")
//      }
    	println(f._1+":"+f._2(0))
      })

      val rdd1MapParRDD = rdd1.mapPartitions(f ⇒ {
        var buf1 = scala.collection.mutable.ArrayBuffer.empty[Float]
        //    	 var resultMap = Map[String,Array[Float]]()
        //    	 var result = List[Array[Float]]()
        var result = scala.collection.mutable.ArrayBuffer.empty[Array[Float]]
        //        f.map(f⇒{
        while (f.hasNext) {
          buf1.appendAll(f.next()._2)
        }
        //        })
        val arr = buf1.toArray
        //        arr.iterator
        //        result.::(arr).iterator
        result.+=(arr).iterator
        //        arr.toIterator
      })
      //Partitions的数量
      println("rdd1MapParRDD的大小是：" + rdd1MapParRDD.count())
      rdd1MapParRDD.collect.foreach(f ⇒ {
        println("rdd1MapParRDD---------------------")
        for (i <- 0 until f.length) {
          print(f(i) + ",")
        }
        println()
      })

      /*
      val broadRddMapParRDD = broadRdd.mapPartitions(f ⇒ {
        var buf1 = scala.collection.mutable.ArrayBuffer.empty[Float]
        //    	 var resultMap = Map[String,Array[Float]]()
        //    	 var result = List[Array[Float]]()
        var result = scala.collection.mutable.ArrayBuffer.empty[Array[Float]]
        //        f.map(f⇒{
        while (f.hasNext) {
          buf1.appendAll(f.next()._2)
        }
        //        })
        val arr = buf1.toArray
        //        arr.iterator
        //        result.::(arr).iterator
        result.+=(arr).iterator
        //        arr.toIterator
      })
      println("broadRddMapParRDD的大小是：" + rdd1MapParRDD.count())
      broadRddMapParRDD.collect.foreach(f ⇒ {
        println("broadRddMapParRDD---------------------")
        for (i <- 0 until f.length) {
          print(f(i) + ",")
        }
        println()
      })
*/
      val caRDD = rdd1MapParRDD.cartesian(broadRdd).map(f⇒(f._1,f._2._2))
      println("caRDD的大小是：" + caRDD.count())
       caRDD.collect.foreach(f ⇒ {
        println("caRDD---------------------")
        println(Arrays.toString(f._1)+"---：：：----"+Arrays.toString(f._2))
      })

      val resultRdd = caRDD.map(f ⇒ {
        //      var tuple=new Tuple2[IntBuffer,FloatBuffer]()
//    	  var list: java.util.List[ReIdAttributesTemp] = new ArrayList
    	   var list= java.util.Collections.synchronizedList(new ArrayList[ReIdAttributesTemp]);
        //        while (f.hasNext) {
        //          var ele = f.next()
        if (f._1.length > 0 && f._2.length > 0) {
          synchronized {
            var javaKnn: JavaKnn = new JavaKnn();
            try {
//              list = javaKnn.getKnn(f._1, f._2, col, minK + 1, javaKnn)
            } catch {
              case e: Exception => println("exception caught: " + e);
            } finally {

              javaKnn = null
            }
          }
        }
//        }
//        list.asScala.toIterator
          list
      })
      println("resultRdd的大小是：" + resultRdd.count())
      val rdd3 = resultRdd.filter(f ⇒ f.size() != 0).flatMap(f ⇒ f.asScala).filter(f ⇒ f != null)
       println("rdd3的大小是：" + rdd3.count())
      rdd3.collect.foreach(f ⇒ {
        println("rdd3---------------------")
        //        for(i <- 0 until f.size()){
        if(f!=null){
        println("arr1:" + f.getFloatArr1()(0) + ",arr1Num:" + f.getFloatArrLineNum1
          + ",arr2:" + f.getFloatArr2()(0) + ",arr2Num:" + f.getFloatArrLineNum2 + ",sim:" + f.getSim)
        //        }
      }})
      val rdd2 = rdd3.map(f ⇒ (f.getFloatArr1, f.getFloatArr2, f.getSim)).filter(f ⇒ (f._3 != 0.0))
      println("rdd2的大小是：" + rdd2.count())
      rdd2.collect.foreach(f ⇒ {
        println("rdd2---------------------")
        println(Arrays.toString(f._1) + "---：：：----" + Arrays.toString(f._2))
      })
      
/*      val rdd3 = rdd2.cartesian(rdd1)
//      println("rdd3的大小是：" + rdd3.count())
rdd3.collect.foreach(f ⇒ {
        println("rdd3---------------------")
        println(Arrays.toString(f._1._1)+"---：：：----"+f._2._1)
      })
      
      val rdd4 = rdd2.cartesian(broadRdd)
//      println("rdd4的大小是：" + rdd4.count())
rdd4.collect.foreach(f ⇒ {
        println("rdd4---------------------")
        println(Arrays.toString(f._1._1)+"---：：：----"+f._2._1)
      })
      
      val rdd5 = rdd3.filter(f ⇒ (Arrays.equals(f._1._1, f._2._2)))
//      println("rdd5的大小是：" + rdd5.count())
rdd5.collect.foreach(f ⇒ {
        println("rdd5---------------------")
        println(Arrays.toString(f._1._1)+"---：：：----"+f._2._1)
      })
      
      val rdd6 = rdd4.filter(f ⇒ (Arrays.equals(f._1._2, f._2._2)))
//      println("rdd6的大小是：" + rdd6.count())
      rdd6.collect.foreach(f ⇒ {
        println("rdd6---------------------")
        println(Arrays.toString(f._1._1)+"---：：：----"+f._2._1)
      })*/
      /*  println("buf1的大小是："+buf1.size)
      buf1.foreach(f⇒{
        println("buf1---------------------")
        println(f)
        })*/
        
      /*  ef.mapPartitions(f⇒{
          var result = List[Float]()
            var i = 0.0f
         while(f.hasNext){
        	 println("f的大小是："+f.size)
           println("ef partition:"+f.next())
         i += f.next()
         }
         result.::(i).iterator
          
        })
      //mapPartitions会重新分区为默认的48个
      val rdd5=ef.mapPartitions(f⇒{
        var result = List[Float]()
      var i = 0.0f
         while(f.hasNext){
         i += f.next()
         }
         result.::(i).iterator
      })
      rdd5.collect().foreach(f⇒{
      println("rdd5:------------------------")
      println(f)
      })*/
      /*var buf2 = scala.collection.mutable.ArrayBuffer.empty[Float]
      broadRdd.map(f⇒{
          
        	buf2.appendAll(f)
      })*/
       
      
//    val rdd2=rdd1.mapPartitions(r⇒r.flatMap(r⇒r))
    /*val rdd1FlatMap=rdd1.flatMap(f⇒f.toList)
    rdd1FlatMap.collect().foreach(f⇒{
    	println("rdd1FlatMap:-------------------------------")
      println(f)
      })
    val broadRddFlatMap=broadRdd.flatMap(f⇒f.toList)
    broadRddFlatMap.collect().foreach(f⇒{
    	println("broadRddFlatMap:-------------------------------")
      println(f)
      })
    val caRDD =rdd1FlatMap .cartesian(broadRddFlatMap)*/
//      val arr1=buf1.toArray
//       println("arr1:"+arr1.size)
////      val arr1Rdd=sc.parallelize(arr1)
//      val arr2=buf2.toArray
//      println("arr2:"+arr2.size)
//      val arr2Rdd=sc.parallelize(arr2)
      //string 是广播变量的id，Array[Float]是总的rdd1的
   /* val caRDD=broadRdd.cartesian(rdd1).map(f⇒(f._1._1,f._2))
    caRDD.collect().foreach(f⇒{
    	println("caRDD:-------------------------------")
//    	for(i <- 0 until f._2.length ){
//          print(f._1+":"+f._2(i)+",")
//      }
    	println(f._1+":"+f._2(0))
      })
    val groupRdd=caRDD.groupByKey()
     groupRdd.collect().foreach(f⇒{
    	println("groupRdd:-------------------------------")
//    	    	for(i <- 0 until f._2.size ){
          println(f._1+":"+f._2+",")
//      }
//    	println()
      })
    val rdd5=groupRdd.map(f⇒f._2.toList)
    rdd5.collect().foreach(f⇒{
    	println("rdd5:-------------------------------")
      println(f)
      })
    val rdd6=rdd5.flatMap(f⇒f.flatten)
     rdd6.collect().foreach(f⇒{
    	println("rdd6:-------------------------------")
//    	for(i <- 0 until f.length ){
//          print(f(i)+",")
//      }
    	println(f)
      })
      println("rdd6的大小是："+rdd6.count())
      */
   
    /*
   val resultBuf= caRDD.map(f⇒{
      var javaKnn:JavaKnn=new JavaKnn()
//       var indexBuf:IntBuffer  = new IntBuffer();
//       var distsBuf: FloatBuffer  = new FloatBuffer();
      javaKnn.getKnn(f._1, arr1Row.intValue(), f._2, arr2Row.intValue(), col, minK, javaKnn);
      // Get results.
       val indexMat: Mat  = javaKnn.getIndexMat();
       val distsMat: Mat  = javaKnn.getDistMat();
        //galleryArray中的位置
       val indexBuf: IntBuffer  = indexMat.getIntBuffer();
        //欧式距离的平方
      val distsBuf:  FloatBuffer  = distsMat.getFloatBuffer();
      (indexBuf,distsBuf)
        
    })
    
    
    resultBuf.map(f⇒{
      f._1
    })
    val rdd2=caRDD.filter(r⇒r._1._1 != r._2._1).filter(r⇒(r._1._2!=null)&&(r._2._2!=null))
    
     println("rdd2的个数是："+rdd2.count())
     
    rdd2.collect().foreach(r⇒{
    	println("rdd2:-------------------------------")
      println(r)
      })
//    rdd2.collect().foreach(println)
//    val start=getCurrent_time
//    val rdd3=rdd2.map(r⇒(r._1._1,r._2._1,euclidean(r._1._2,r._2._2)))
//    val rdd3=rdd2.map{r⇒(r._2._1,(r._1._1,euclidean(r._1._2,r._2._2)))}
    val startTime = System.currentTimeMillis();
    rdd2.foreach(r⇒(r._1._1,euclidean(r._1._2,r._2._2)))
		val EndTime=System.currentTimeMillis();
		println("Cost time of euclidean: " + (EndTime-startTime) + "ms")
    val rdd3=rdd2.mapPartitions(r⇒{
      r.map(r⇒(r._2._1,(r._1._1,euclidean(r._1._2,r._2._2))))
      
      })
    val rdd3=rdd2.map{r⇒{
      
     var settings:FloatPointerDemo.Settings  = new FloatPointerDemo.Settings();
		var finder:FloatPointerDemo  = new FloatPointerDemo(settings);
		println(getMat(r._1._2))
		println(getMat(r._2._2))
		finder.flannFindPairs(getMat(r._1._2),getMat(r._2._2));
    }
      }
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
    println("rdd3的个数是："+rdd3.count())
    rdd3.collect().foreach(f⇒{
    	println("rdd3:-------------------------------")
      println(f)
      })
    val b=rdd3.groupByKey()
    b.collect().foreach(f⇒{
    	println("b:-------------------------------")
      println(f)
      })
   val c= b.map(f⇒(f._1,(f._2.toList.sortBy(f⇒f._2))))
   c.collect().foreach(f⇒{
    	println("c:-------------------------------")
      println(f)
      })
   
   //计算top的时间
//      val TOP3startTime = System.currentTimeMillis();
    val result=c.map(f⇒(f._1,f._2.take(3)))
    
//     val TOP3EndTime=System.currentTimeMillis();
    //top10
//    val result10=c.map(f⇒(f._1,f._2.take(10)))
    
//    val TOP10EndTime=System.currentTimeMillis();
//    println("Cost time of TOP3: " + (TOP3EndTime-TOP3startTime) + "ms")
//    println("Cost time of TOP10: " + (TOP10EndTime-TOP3EndTime) + "ms")
    
//    val endTop =getCurrent_time
//    println("topk用时:"+endTop+"-"+startTop+"="+(endTop-startTop))
    
    println("topk 结束")
//    println("result的个数是："+result.count())
//    val dbstartTime = System.currentTimeMillis();
//    val resultArr= result.collect()
    result.collect().foreach(f⇒{
    	println("result:-------------------------------")
      println(f)
      })
//    val operationEndTime=System.currentTimeMillis();
//    val operationEveryTime=operationEndTime - dbstartTime
//    println("Cost time of every operation: " + (operationEveryTime) + "ms")

    //测试不收集会不会执行add操作
    //闭包，没用的
//    var i=0
    
    //依然是spark任务
    result.foreachPartition(f⇒{
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
      
    })
    //foreach好像不再是spark任务
      result.foreachPartition(f⇒{
    	  
    	  var dbConnector:GraphDatabaseConnector=new Neo4jConnector();
    	f.foreach(f⇒{
    	  //循环的次数就是节点的个数
//    		println("dbConnector-"+i+":"+dbConnector.toString)
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
            println("min需要保存的结果是：[{'sim':"+f._2(i)._2+",'trackletID1':'"+f._1+"','trackletID2':'"+f._2(i)._1+"'}]")
//            println("dbConnector的开启状态："+dbConnector.isOpen)
            if(!(f._2(i)._2.toString().equals("null"))){
              
            	val outlist=dbConnector.addSimRel(f._1, f._2(i)._1, f._2(i)._2)
            			println("min保存完成的结果是："+outlist.toString())
            }
        }
//        		i =i+ 1
      })
        	dbConnector.finalize()

    	    dbConnector=null
//    	println("内层foreach结束---------------------------")
      
    })
    result.unpersist()
    
//        		println("i:"+i)
//    println("外层foreachPartition结束---------------------------")
//     val dbendTime = System.currentTimeMillis();
//     val dbEveryTime=dbendTime - dbstartTime
//     println("Cost time of erery db: " + (dbEveryTime) + "ms")
//     println("min result:-------------------------------")
//     return (operationEveryTime,dbEveryTime)
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
*/     }

else if(args(0).equals("hour")){
      val rdd1 = rdd.map(r ⇒ ((r.getTrackletID, r.getFeatureVector,r.getStart)))
   /* rdd1.collect().foreach(f⇒{
    	println("rdd1:-------------------------------")
      println(f)
      })*/
   /* var broadArr=broad.value
    for(i<- 0 until broadArr.length){
      
    }*/
    val broadRdd=sc.parallelize(broad.value).map(r ⇒ ((r.getTrackletID, r.getFeatureVector,r.getStart)))
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
    var rdd3:RDD[(String, (String, Double))]=null
    if(rdd2!=null){
      
//    	rdd3=rdd2.map{case r⇒(r._2._1,(r._1._1,euclidean(r._1._2,r._2._2)))}
    
      rdd3=rdd2.mapPartitions(r⇒{
      r.map(r⇒(r._2._1,(r._1._1,euclidean(r._1._2,r._2._2))))
      
      })
      }
//      rdd3.cache
      rdd3.persist(StorageLevel.MEMORY_AND_DISK)
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
      dbConnector.release()
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
   
//      val TOP3startTime = System.currentTimeMillis();
    val result=c.map(f⇒(f._1,f._2.take(3)))
//     val TOP3EndTime=System.currentTimeMillis();
//    val result10=c.map(f⇒(f._1,f._2.take(10)))
//    val TOP10EndTime=System.currentTimeMillis();
//    println("Cost time of TOP3: " + (TOP3EndTime-TOP3startTime) + "ms")
//    println("Cost time of TOP10: " + (TOP10EndTime-TOP3EndTime) + "ms")
//    println("result的个数是："+result.count())
    
    println("topk 结束")
    
//    val dbstartTime = System.currentTimeMillis();
//    val resultArr= result.collect()
//    val operationEndTime=System.currentTimeMillis();
//    val operationEveryTime=operationEndTime - dbstartTime
//    println("Cost time of every operation: " + (operationEveryTime) + "ms")
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
            
             val outlist=dbConnector.addHourSimRel(f._1, f._2(i)._1, f._2(i)._2,args(1))
            println("hour保存完成的结果是："+outlist.toString())
        }
    		
      })
    	dbConnector.release()
    	dbConnector=null
    })
    result.unpersist()
    rdd3.unpersist()
//     val dbendTime = System.currentTimeMillis();
//     val dbEveryTime=dbendTime - dbstartTime
//     println("Cost time of erery db: " + (dbEveryTime) + "ms")
//     println("result:-------------------------------")
//     return (operationEveryTime,dbEveryTime)
     
    }
    }
  
  //笛卡尔积和Broadcast，每一个都要有KNN
  def everyOneNeedEuDis(rdd: RDD[(ReIdAttributesTemp, Long)],broad:Broadcast[Array[(ReIdAttributesTemp, Long)]]
//          ,dbConnectorByte:Array[Byte]  
//  ,dbConnSingleton:SingletonUtil[GraphDatabaseConnector]
  , args:Array[String]
  ) :RDD[(String, String, Double)]= {
//    val dbConnector=getConnector(dbConnectorByte)
//    rdd.collect().foreach{ReIdAttributesTemp⇒{
//      val fea=ReIdAttributesTemp.getFeatureVector
//      println(ReIdAttributesTemp.getTrackletID+","+fea(0))
//      }}
    
if(args(0).equals("minute")){
      
    
    val rdd1 = rdd.map(r ⇒ ((r._1.getTrackletID, r._1.getFeatureVector),r._2))
    /*println("rdd1的个数是："+rdd1.count())
    rdd1.collect().foreach(f⇒{
    	println("rdd1:-------------------------------")
      println(f)
      })*/
   /* var broadArr=broad.value
    for(i<- 0 until broadArr.length){
      
    }*/
      //.repartition(partition)
    val broadRdd=sc.parallelize(broad.value).map(r ⇒ ((r._1.getTrackletID, r._1.getFeatureVector),r._2))
    /*println("broadRdd的个数是："+broadRdd.count())
    broadRdd.collect().foreach(f⇒{
    	println("broadRdd:-------------------------------")
      println(f)
      })*/
    val caRDD = rdd1.cartesian(broadRdd)
    /*println("caRDD的个数是："+caRDD.count())
    caRDD.collect().foreach(f⇒{
    	println("caRDD:-------------------------------")
      println(f)
      })*/
//    val rdd2=caRDD.filter(r⇒r._1._1 != r._2._1).filter(r⇒(r._1._2!=null)&&(r._2._2!=null))
    val rdd2=caRDD.filter(r⇒r._1._1._1 != r._2._1._1).filter(r⇒r._1._2<r._2._2).filter(r⇒(r._1._1._2!=null)&&(r._2._1._2!=null))
     /*println("rdd2的个数是："+rdd2.count())
     
    rdd2.collect().foreach(r⇒{
    	println("rdd2:-------------------------------")
      println(r)
      })*/
//    rdd2.collect().foreach(println)
//    val start=getCurrent_time
//    val rdd3=rdd2.map(r⇒(r._1._1,r._2._1,euclidean(r._1._2,r._2._2)))
//    val rdd3=rdd2.map{r⇒(r._2._1,(r._1._1,euclidean(r._1._2,r._2._2)))}
    /*val startTime = System.currentTimeMillis();
    rdd2.foreach(r⇒(r._1._1,euclidean(r._1._2,r._2._2)))
		val EndTime=System.currentTimeMillis();
		println("Cost time of euclidean: " + (EndTime-startTime) + "ms")*/
    val rdd3=rdd2.map(r⇒(r._1._1._1,r._2._1._1,euclidean(r._1._1._2,r._2._1._2)))
     /* println("rdd3的个数是："+rdd3.count())
    rdd3.collect().foreach(f⇒{
    	println("rdd3:-------------------------------")
      println(f)
      })*/
    rdd3
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
    
    
    
    
    
/*    val b=rdd3.groupByKey()
    b.collect().foreach(f⇒{
    	println("b:-------------------------------")
      println(f)
      })
   val c= b.map(f⇒(f._1,(f._2.toList.sortBy(f⇒f._2))))
   c.collect().foreach(f⇒{
    	println("c:-------------------------------")
      println(f)
      })
   
   //计算top的时间
//      val TOP3startTime = System.currentTimeMillis();
    val result=c.map(f⇒(f._1,f._2.take(3)))
    
//     val TOP3EndTime=System.currentTimeMillis();
    //top10
//    val result10=c.map(f⇒(f._1,f._2.take(10)))
    
//    val TOP10EndTime=System.currentTimeMillis();
//    println("Cost time of TOP3: " + (TOP3EndTime-TOP3startTime) + "ms")
//    println("Cost time of TOP10: " + (TOP10EndTime-TOP3EndTime) + "ms")
    
//    val endTop =getCurrent_time
//    println("topk用时:"+endTop+"-"+startTop+"="+(endTop-startTop))
    
    println("topk 结束")
//    println("result的个数是："+result.count())
//    val dbstartTime = System.currentTimeMillis();
//    val resultArr= result.collect()
    result.collect().foreach(f⇒{
    	println("result:-------------------------------")
      println(f)
      })
//    val operationEndTime=System.currentTimeMillis();
//    val operationEveryTime=operationEndTime - dbstartTime
//    println("Cost time of every operation: " + (operationEveryTime) + "ms")

    //测试不收集会不会执行add操作
    //闭包，没用的
//    var i=0
    
    //依然是spark任务
    result.foreachPartition(f⇒{
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
      
    })
    //foreach好像不再是spark任务
      result.foreachPartition(f⇒{
    	  
    	  var dbConnector:GraphDatabaseConnector=new Neo4jConnector();
    	f.foreach(f⇒{
    	  //循环的次数就是节点的个数
//    		println("dbConnector-"+i+":"+dbConnector.toString)
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
            println("min需要保存的结果是：[{'sim':"+f._2(i)._2+",'trackletID1':'"+f._1+"','trackletID2':'"+f._2(i)._1+"'}]")
//            println("dbConnector的开启状态："+dbConnector.isOpen)
            if(!(f._2(i)._2.toString().equals("null"))){
              
            	val outlist=dbConnector.addSimRel(f._1, f._2(i)._1, f._2(i)._2)
            			println("min保存完成的结果是："+outlist.toString())
            }
        }
//        		i =i+ 1
      })
        	dbConnector.finalize()

    	    dbConnector=null
//    	println("内层foreach结束---------------------------")
      
    })
    result.unpersist()
    
//        		println("i:"+i)
//    println("外层foreachPartition结束---------------------------")
//     val dbendTime = System.currentTimeMillis();
//     val dbEveryTime=dbendTime - dbstartTime
//     println("Cost time of erery db: " + (dbEveryTime) + "ms")
//     println("min result:-------------------------------")
//     return (operationEveryTime,dbEveryTime)
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
    (1,1)*/
     }

/*else if(args(0).equals("hour")){
      val rdd1 = rdd.map(r ⇒ ((r.getTrackletID, r.getFeatureVector,r.getStart)))
    rdd1.collect().foreach(f⇒{
    	println("rdd1:-------------------------------")
      println(f)
      })
    var broadArr=broad.value
    for(i<- 0 until broadArr.length){
      
    }
    val broadRdd=sc.parallelize(broad.value).map(r ⇒ ((r.getTrackletID, r.getFeatureVector,r.getStart)))
    println("broadRdd的个数是："+broadRdd.count())
    broadRdd.collect().foreach(f⇒{
    	println("broadRdd:-------------------------------")
      println(f)
      })
    val caRDD = rdd1.cartesian(broadRdd)
    caRDD.collect().foreach(f⇒{
    	println("caRDD:-------------------------------")
      println(f)
      })
    val rdd2=caRDD.filter(r⇒r._1._1 != r._2._1).filter(r⇒(r._1._2!=null)&&(r._2._2!=null)&&(r._1._3!=r._2._3))
    
     println("rdd2的个数是："+rdd2.count())
    rdd2.collect().foreach(r⇒{
    	println("rdd2:-------------------------------")
      println(r)
      })
//    rdd2.collect().foreach(println)
//    val start=getCurrent_time
//    val rdd3=rdd2.map(r⇒(r._1._1,r._2._1,euclidean(r._1._2,r._2._2)))
    var rdd3:RDD[(String, (String, Double))]=null
    if(rdd2!=null){
      
//    	rdd3=rdd2.map{case r⇒(r._2._1,(r._1._1,euclidean(r._1._2,r._2._2)))}
    
      rdd3=rdd2.mapPartitions(r⇒{
      r.map(r⇒(r._2._1,(r._1._1,euclidean(r._1._2,r._2._2))))
      
      })
      }
//      rdd3.cache
      rdd3.persist(StorageLevel.MEMORY_AND_DISK)
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
    println("rdd3的个数是："+rdd3.count())
    rdd3.collect().foreach(f⇒{
    	println("rdd3:-------------------------------")
      println(f)
      })
    val b=rdd3.groupByKey()
    println("b的个数是："+b.count())
    b.collect().foreach(f⇒{
    	println("b:-------------------------------")
      println(f)
      })
    
     //必须收集才可以将work上的信息拿回来
    
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
      })
      
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
   e.collect().foreach(f⇒{
    	for(i <- 0 until f.size()){
    		println("e:-------------------------------")
    	  println(f.get(i).toString())
    	}
      })
    
      val rdd6=e.flatMap(f⇒f.asScala).map(f⇒(f.getTrackletID1,(f.getTrackletID2,f.getSim)))
//        for(i <- 0 until f.size()){
////        (f.get(i).getTrackletID1,(f.get(i).getTrackletID2,f.get(i).getSim))
//          f.get(i)
//        }
        
      
      rdd6.collect().foreach(f⇒{
    	println("rdd6:-------------------------------")
      println(f)
      })
      //RDD[(String, Iterable[(String, Double)])]中的Iterable不能union
//    val rdd5=b.union(rdd6)
//    val rdd5=rdd3.union(rdd6)
    rdd5.collect().foreach(f⇒{
    	println("rdd5:-------------------------------")
      println(f)
      })
      //直接取top3是不对是，这样取出来的不是每一个节点的top3，而是这一次遍历的top3，而且这样，在后续插入数据库的时候，效率很低
//    val result=rdd5.top(3)(Ordering.by[(String, (String,Double)), Double](_._2._2).reverse)  
    
    val rdd5=rdd3.union(rdd6).groupByKey()
   val c= rdd5.map(f⇒(f._1,(f._2.toList.sortBy(f⇒f._2))))
   c.collect().foreach(f⇒{
    	println("c:-------------------------------")
      println(f)
      })
   
//      val TOP3startTime = System.currentTimeMillis();
    val result=c.map(f⇒(f._1,f._2.take(3)))
//     val TOP3EndTime=System.currentTimeMillis();
//    val result10=c.map(f⇒(f._1,f._2.take(10)))
//    val TOP10EndTime=System.currentTimeMillis();
//    println("Cost time of TOP3: " + (TOP3EndTime-TOP3startTime) + "ms")
//    println("Cost time of TOP10: " + (TOP10EndTime-TOP3EndTime) + "ms")
//    println("result的个数是："+result.count())
    
    println("topk 结束")
    
//    val dbstartTime = System.currentTimeMillis();
//    val resultArr= result.collect()
//    val operationEndTime=System.currentTimeMillis();
//    val operationEveryTime=operationEndTime - dbstartTime
//    println("Cost time of every operation: " + (operationEveryTime) + "ms")
    //如果直接使用rdd5.top(3),则result不再是rdd，所以直接foreach，未来看是否再使用sc变成rdd
    result.foreachPartition(f⇒{
      
    	var dbConnector=new Neo4jConnector();
    	f.foreach(f⇒{
//    		println("dbConnector:"+dbConnector.toString)
    		val db=new Factory[Neo4jConnector](){
    			
    			def produce() :Neo4jConnector={
    					return new Neo4jConnector()
    			}
    			
    		};
    		val dbConnSingleton=new SingletonUtil[Neo4jConnector](db, classOf[Neo4jConnector]);
    		val dbConnector=dbConnSingleton.getInst()
       
        for(i <- 0 until f._2.length){
            println("hour需要保存的结果是：[{'sim':"+f._2(i)._2+",'trackletID1':'"+f._1+"','trackletID2':'"+f._2(i)._1+"'}]")
            
             val outlist=dbConnector.addHourSimRel(f._1, f._2(i)._1, f._2(i)._2)
            println("hour保存完成的结果是："+outlist.toString())
        }
    		
      })
    	dbConnector.finalize()
    	dbConnector=null
    })
    result.unpersist()
    rdd3.unpersist()
//     val dbendTime = System.currentTimeMillis();
//     val dbEveryTime=dbendTime - dbstartTime
//     println("Cost time of erery db: " + (dbEveryTime) + "ms")
//     println("result:-------------------------------")
//     return (operationEveryTime,dbEveryTime)
     (1,1)
    }*/
else {
 
    null
}
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


/*case class Simi(
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
}*/



    