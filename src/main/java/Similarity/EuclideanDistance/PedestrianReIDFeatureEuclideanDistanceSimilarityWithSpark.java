package Similarity.EuclideanDistance;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;

import Similarity.EuclideanDistance.util.ConsoleLogger;
import Similarity.EuclideanDistance.util.Logger;
import Similarity.EuclideanDistance.util.SerializationHelper;
import Similarity.EuclideanDistance.util.SingletonUtil;
import demo.Similarity;
import entities.Hour;
import entities.Minute;
import entities.ReIdAttributesTemp;

public class PedestrianReIDFeatureEuclideanDistanceSimilarityWithSpark implements Serializable
//extends SparkStreamingApp
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 411317335024542242L;
//	public static final String APP_NAME = "reid-similarity";
	public static Logger logger=new ConsoleLogger();
	public GraphDatabaseConnector dbConnector=null;
	public SingletonUtil<GraphDatabaseConnector> dbConnSingleton=null;
	Similarity similarity=null;
//	public final Driver driver = GraphDatabase.driver("bolt://172.18.33.37:7687",
//            AuthTokens.basic("neo4j", "casia@1234"));
//	public final Session session = driver.session();
//	public final GraphDatabaseConnector dbConnector = new Neo4jConnector(driver,session);
	public void init() throws Exception{
		
//		dbConnSingleton=new SingletonUtil<>(Neo4jConnector::new, Neo4jConnector.class);
//		dbConnector=dbConnSingleton.getInst();
		dbConnector=new Neo4jConnector();
		similarity=new Similarity();
	//	final Logger logger = loggerSingleton.getInst();
	}
//	static SparkConf conf=new SparkConf().setMaster("spark://rtask-nod8:7077").setAppName("Euclidean-Distance");
//	static JavaSparkContext sc=new JavaSparkContext(conf);
//	public PedestrianReIDFeatureEuclideanDistanceSimilarity(SystemPropertyCenter propCenter) throws Exception{
//		super(propCenter, APP_NAME);
//	}

	public static GraphDatabaseConnector getConnector(byte [] dbConnectorByte) throws Exception{
		return SerializationHelper.deserialize(dbConnectorByte);
	}
	public static double getSim(float[] a, float[] b) {
		double distance = 0;

		for (int i = 0; i < a.length; i++) {
			double temp = Math.pow((a[i] - b[i]), 2);
			distance += temp;
		}
//		System.out.println(distance);
		return distance;
		
		/*由于并不需要真正的相似度，所以只需要距离，那么距离越小，则越近
		 * distance = Math.sqrt(distance);
		return 1.0 / (1.0 + distance);*/
	}

	@SuppressWarnings("unchecked")
	public void getSim(List<ReIdAttributesTemp> list
//			,JavaSparkContext sc
			,String[] args
			) throws Exception {
		// 前k个
//		int k = 50;
//		listToVector(list);
//		similarity(list);
		;
		//spark		
		long SparkstartTime = System.currentTimeMillis();
//		Similarity similarity=null;
//		similarity=new Similarity();
		similarity.glom(similarity.listToRdd(list),args);
		long SparkendTime = System.currentTimeMillis();
		logger.info("Cost time of spark: " + (SparkendTime - SparkstartTime) + "ms");
		
//		printBroadcastList();
//		EuDis(rdd);
//		RDD<Simi> simi=
//		foreachPrint(simi);
		/*List wordslist = simi.collect();
		for (int i = 0; i < wordslist.size(); i++) {
			System.out.println("words--------------------------------");
			System.out.println(wordslist.get(i));

		}
		JavaRDD<ReIdAttributesTemp> javaRDD = sc.parallelize(list);
		
		JavaRDD<String> logData1 = javaRDD.map(new Function<ReIdAttributesTemp, String>() {

			@Override
			public String call(ReIdAttributesTemp v1) throws Exception {
				// TODO Auto-generated method stub
				return v1.toString();
			}

		});
		JavaPairRDD<String, Float> ones = javaRDD.mapToPair(
			      new PairFunction<ReIdAttributesTemp, String, Float>() {
			        public Tuple2<String, Float> call(ReIdAttributesTemp s) {
			          return new Tuple2<String, Float>(s.getTrackletID(), s.getFeatureVector()[0]);
			        }
			      });
		List<JavaRDD<ReIdAttributesTemp>> javaRDDsList=new ArrayList<>();
		javaRDDsList.add(javaRDD);
		javaRDDsList.add(javaRDD);
		javaRDDsList.forEach(new Consumer<JavaRDD<ReIdAttributesTemp>>() {

			@Override
			public void accept(JavaRDD<ReIdAttributesTemp> t) {
				// TODO Auto-generated method stub
				
			}
		});
		for (int i = 0; i < javaRDDsList.size(); i++) {
			JavaPairRDD<ReIdAttributesTemp, ReIdAttributesTemp> ones = javaRDDsList.get(i).mapToPair(
					new PairFunction<ReIdAttributesTemp, ReIdAttributesTemp, ReIdAttributesTemp>() {
						public Tuple2<ReIdAttributesTemp, ReIdAttributesTemp> call(ReIdAttributesTemp s) {
							return new Tuple2<ReIdAttributesTemp, ReIdAttributesTemp>(s, s);
						}
					});
		}
		List<ReIdAttributesTemp> uList=new ArrayList<>();
		 JavaRDD<ReIdAttributesTemp> words = javaRDD.flatMap(new FlatMapFunction<ReIdAttributesTemp, ReIdAttributesTemp>() {
		      public Iterator<ReIdAttributesTemp> call(ReIdAttributesTemp s) {
		    	  uList.add(s);
		        return uList.iterator();
		      }
		    });
		 
		 
		JavaPairRDD<ReIdAttributesTemp, ReIdAttributesTemp> ones = words.mapToPair(
				new PairFunction<ReIdAttributesTemp, ReIdAttributesTemp, ReIdAttributesTemp>() {
					public Tuple2<ReIdAttributesTemp, ReIdAttributesTemp> call(ReIdAttributesTemp s) {
						return new Tuple2<ReIdAttributesTemp, ReIdAttributesTemp>(s, s);
					}
				});
//		List outlist = ones.join(ones).collect();
		JavaRDD<Double> double1=ones.map(new Function<Tuple2<ReIdAttributesTemp, ReIdAttributesTemp>, Double>() {

			@Override
			public Double call(Tuple2<ReIdAttributesTemp, ReIdAttributesTemp> v1) throws Exception {
				// TODO Auto-generated method stub
				ReIdAttributesTemp r1=v1._1();
				ReIdAttributesTemp r2=v1._2();
				return getSim(r1.getFeatureVector(), r2.getFeatureVector());
			}
			
		});
		List outlist = double1.collect();
		for (int i = 0; i < outlist.size(); i++) {
			System.out.println("--------------------------------");
			System.out.println(outlist.get(i));

		}*/
		
		/*long startTime = System.currentTimeMillis();
		MultiKeyMap<String, Double> multiKeyMap=new MultiKeyMap<>();
//		ReIdAttributesTemp reIdAttributesTemp[][]=new ReIdAttributesTemp[list.size()][list.size()];
		List<ReIdAttributesTempRDD> reIdAttributesTempRDDList=new ArrayList<>();
		for (int i = 0; i < list.size(); i++) {
			for (int j = i + 1; j < list.size(); j++) {
//				 System.out.println(String.valueOf(list.get(i)[0])+":"+String.valueOf(list.get(i)[1]));
//				 System.out.println(String.valueOf(list.get(j)[0])+":"+String.valueOf(list.get(j)[1]));
				//目前getCamID是相同的，所以测试时为equals
				if ((list.get(i).getCamID().equals(list.get(j).getCamID()))) {
					if (list.get(i).getFeatureVector()!=null&&list.get(j).getFeatureVector()!=null) {
//						javaRDD.map(new Function2<T1, T2, R>(){
//							
//						});
//						reIdAttributesTemp[i][j]=
						ReIdAttributesTempRDD reIdAttributesTempRDD=
								new ReIdAttributesTempRDD(i,j,list.get(i).getTrackletID(), list.get(j).getTrackletID(),list.get(i),list.get(j));
						reIdAttributesTempRDDList.add(reIdAttributesTempRDD);
						double sim=getSim(list.get(i).getFeatureVector(), list.get(j).getFeatureVector());
						multiKeyMap.put(list.get(i).getTrackletID(), list.get(j).getTrackletID(),sim);
						
					}
				}
			}
		}
		
//		EuDis2(listToRdd(reIdAttributesTempRDDList));
		
		List<Entry<MultiKey<? extends String>, Double>> multiKeyMapList=
				new ArrayList<Entry<MultiKey<? extends String>, Double>>(multiKeyMap.entrySet());
		Collections.sort(multiKeyMapList,new Comparator<Entry<MultiKey<? extends String>, Double>>(){
			public int compare(Entry<MultiKey<? extends String>, Double> o1,
					Entry<MultiKey<? extends String>, Double> o2) {
				double sim1Value=o1.getValue().doubleValue();
				double sim2Value=o2.getValue().doubleValue();
				if (sim1Value > sim2Value) {
					return 1;
				} else if (sim1Value == sim2Value) {
					return 0;
				} else {
					return -1;
				}
            }
		});
		for (Entry<MultiKey<? extends String>, Double> entry : multiKeyMapList) {
			System.out.println(entry.getValue().doubleValue()+","+entry.getKey().getKey(0)+","+entry.getKey().getKey(1));
		}
		
		//前k个，目前为了测试，全部添加
		//multiKeyMapList.size()-(multiKeyMapList.size()-k)
		if (k<multiKeyMapList.size()) {
			
			for (int i = 0; i < k ; i++) {
				Entry<MultiKey<? extends String>, Double> entry=multiKeyMapList.get(i);
				MultiKey<? extends String> keys = entry.getKey();
				double sim = entry.getValue().doubleValue();
				String id1 = keys.getKey(0);
				String id2 = keys.getKey(1);
//			System.out.println(i+"sim:"+sim+",id1:"+id1+",id2:"+id2);
				logger.info(i+",id1:"+id1+",id2:"+id2+",sim:"+sim);
//			dbConnector.addIsGetSim(id1, false);
//			dbConnector.addIsGetSim(id2, false);
				long dbstartTime = System.currentTimeMillis();
				dbConnector.addSimRel(id1, id2, sim);
				long dbendTime = System.currentTimeMillis();
				logger.info("Cost time of db: " + (dbendTime - dbstartTime) + "ms");
			}
		}
		long endTime = System.currentTimeMillis();

		logger.info("Cost time of nomal: " + (endTime - startTime) + "ms");*/
	}
	
	public static void main(String[] args) throws Exception  {

		
//		String classpathString=System.getProperty("java.class.path");
//		System.out.println("classpathString:"+classpathString);
		PedestrianReIDFeatureEuclideanDistanceSimilarityWithSpark p=new PedestrianReIDFeatureEuclideanDistanceSimilarityWithSpark();
		p.init();
		p.addToContext(args);
		
//		test();
//		ItemCFtest();
//		a();
//		car();
//		sort();
//		glomTestDemo();
//		new reduceTest().reducetest();
	}

//	@Override
	public void addToContext(String[] args) {

//		SparkConf conf=new SparkConf().setMaster("spark://rtask-nod8:7077").setAppName("Euclidean-Distance");
//		JavaSparkContext sc=new JavaSparkContext(conf);
		//目前去掉由minute循环遍历得到Person节点的过程
		
		try {
			if (args[0].equals("minute")) {
			
		
				List<Minute> minList=dbConnector.getMinutes();
			
				if (minList!=null) {
			
		
					for (int i = 0; i < minList.size(); i++) {
//			System.out.println("minList.get(i):"+i+":"+minList.get(i));
						List<ReIdAttributesTemp> list=dbConnector.getPedestrianReIDFeatureList(minList.get(i));
		
//			List<ReIdAttributesTemp> list=dbConnector.getPedestrianReIDFeatureList(new Minute());
			/*for (int i = 0; i <list.size(); i++) {
				System.out.println("out:--------------------------------");
				System.out.println(list.get(i).toString());
				
			}*/
		/*JavaRDD<ReIdAttributesTemp> javaRDD = sc.parallelize(list);
		JavaRDD<String> logData1 = javaRDD.map(new Function<ReIdAttributesTemp, String>() {

			@Override
			public String call(ReIdAttributesTemp v1) throws Exception {
				// TODO Auto-generated method stub
				return v1.toString();
			}

		});
		List outlist = logData1.collect();
		for (int i = 0; i < outlist.size(); i++) {
			System.out.println("--------------------------------");
			System.out.println(outlist.get(i));

		}*/
		
		
		/*JavaRDD<Integer> rdd=sc.parallelize(Arrays.asList(1,2,3,4));
		JavaRDD<Integer> result=rdd.map(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer x){ return x*x;}
		});
		System.out.println(StringUtils.join(result.collect(),","));*/
		
//			for (int j = 0; j < list.size(); j++) {
//				System.out.println(list.get(j));
//			}
						if (list!=null) {
							
							if (list.size()>0) {
								System.out.println("找到的minute是："+minList.get(i).toString());
			//				System.out.println(minList.get(i)+":"+list.size());
			//				getSim(outlist);
			//		getSim(list,sc);
								try {
									getSim(list,args);
								} catch (Exception e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
							}
						}
					}
				}
			}if (args[0].equals("hour")) {

				
				
				List<Hour> hourList=dbConnector.getHours();
			
				if (hourList!=null) {
			
		
					for (int i = 0; i < hourList.size(); i++) {
						List<ReIdAttributesTemp> list=dbConnector.getPedestrianReIDFeatureList(hourList.get(i));
				
				/**
				 * 模拟得到hourList下的节点
				 */
				/*List<ReIdAttributesTemp> list=new ArrayList<>();
				List<Minute> minList=dbConnector.getMinutes();
				
				if (minList != null) {
					for (int i = 0; i < minList.size(); i++) {
						List<ReIdAttributesTemp> list2 = dbConnector.getPedestrianReIDFeatureList(minList.get(i));
						if (list2 != null) {

							if (list2.size() > 0) {
								list.addAll(list2);
							}
						}
					}
				}*/
				if (list != null) {

					if (list.size() > 0) {
								System.out.println("找到的hour是："+hourList.get(i).toString());
							
								try {
									getSim(list,args);
								} catch (Exception e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
							}
						}
					}
				}
			
						
			}
		} finally {
			try {
				System.out.println("addToContext finally");
//				dbConnector.finalize();
			} catch (Throwable e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}

}