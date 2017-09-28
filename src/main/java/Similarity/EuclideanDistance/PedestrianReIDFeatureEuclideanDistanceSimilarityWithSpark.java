package Similarity.EuclideanDistance;

import java.io.Serializable;
import java.util.List;

import Similarity.EuclideanDistance.util.ConsoleLogger;
import Similarity.EuclideanDistance.util.Logger;
import Similarity.EuclideanDistance.util.SerializationHelper;
import Similarity.EuclideanDistance.util.SingletonUtil;
import demo.Similarity;
import demo.Similarity2;
import entities.Hour;
import entities.Minute;
import entities.ReIdAttributesTemp;
import test.MapTest;
	/**
	 * 计算相似度
	 */
public class PedestrianReIDFeatureEuclideanDistanceSimilarityWithSpark implements Serializable
{
	private static final long serialVersionUID = 411317335024542242L;
	public static Logger logger=new ConsoleLogger();
	public GraphDatabaseConnector dbConnector=null;
	public SingletonUtil<GraphDatabaseConnector> dbConnSingleton=null;
	Similarity similarity=null;
	Similarity2 similarity2=null;
//	public final Driver driver = GraphDatabase.driver("bolt://172.18.33.37:7687",
//            AuthTokens.basic("neo4j", "casia@1234"));
//	public final Session session = driver.session();
//	public final GraphDatabaseConnector dbConnector = new Neo4jConnector(driver,session);
	public void init() throws Exception{
		
//		dbConnSingleton=new SingletonUtil<>(Neo4jConnector::new, Neo4jConnector.class);
//		dbConnector=dbConnSingleton.getInst();
		dbConnector=new Neo4jConnector();
		similarity=new Similarity();
//		similarity2=new Similarity2();
	//	final Logger logger = loggerSingleton.getInst();
	}

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
		;
		//spark		
		long SparkstartTime = System.currentTimeMillis();
//		similarity2.glom(similarity2.listToRdd(list),args);
		similarity.glomWithFlann(similarity.listToRdd(list),args);
		long SparkendTime = System.currentTimeMillis();
		logger.info("Cost time of spark: " + (SparkendTime - SparkstartTime) + "ms");
		
//		printBroadcastList();
//		EuDis(rdd);
		
		//单机版java实现
		/*
		long startTime = System.currentTimeMillis();
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

		logger.info("Cost time of nomal java: " + (endTime - startTime) + "ms");
		*/
	}
	
	public static void main(String[] args) throws Exception  {

		
//		String classpathString=System.getProperty("java.class.path");
//		System.out.println("classpathString:"+classpathString);
		PedestrianReIDFeatureEuclideanDistanceSimilarityWithSpark p=new PedestrianReIDFeatureEuclideanDistanceSimilarityWithSpark();
		p.init();
		p.dbConnector.copyNodes();
//		p.addToContext(args);
//		MapTest.doubleMap();
//		matrix m=new matrix();
//		m.testmatrix();
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
			
						if (list!=null) {
							
							if (list.size()>0) {
								System.out.println("找到的minute是："+minList.get(i).toString());
			//				System.out.println(minList.get(i)+":"+list.size());
			
								try {
									/*for (int j = 0; j <list.size(); j++) {
										System.out.println("out:--------------------------------");
										System.out.println(list.get(j).toString());
										
									}*/
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
		}catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				dbConnector.release();
				System.out.println("addToContext finally");
			} catch (Throwable e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}

}