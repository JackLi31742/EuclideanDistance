package Similarity.EuclideanDistance;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import db.JdbcUtils4Neo4j;
import entities.ReIdAttributesTemp;

public class Neo4jDaoJdbc implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 9164415191191251933L;
	
	public synchronized List<ReIdAttributesTemp> addSimRel(String nodeID1, String nodeID2, double SimRel, String dataType) {
		long dbstartTime = System.currentTimeMillis();
		
		List<ReIdAttributesTemp> list = new ArrayList<>();
		
		String sql="MATCH (a:Person {trackletID: {1},dataType:'" + dataType
				+ "'}), (b:Person {trackletID: {2},dataType:'" + dataType + "'}) "
				+ "MERGE (a)-[r:Similarity]-(b) set r.Minute={3} "
				+ "return a.trackletID,b.trackletID,r.Minute;";
		PreparedStatement ps = null;
		Connection conn = null;
		ResultSet rs=null;
		try {
			conn = JdbcUtils4Neo4j.getConnection();
			ps = conn.prepareStatement(sql);
			ps.setString(1, nodeID1);
			ps.setString(2, nodeID2);
			ps.setDouble(3, SimRel);
			rs=ps.executeQuery();
			
			
			while (rs.next()) {
				ReIdAttributesTemp reIdAttributesTemp = new ReIdAttributesTemp();
				String trackletID1 = rs.getString("a.trackletID");
				String trackletID2 = rs.getString("b.trackletID");
				Double sim =  rs.getDouble("r.Minute");
				if (!(trackletID1.equals("null"))) {
					reIdAttributesTemp.setTrackletID1(trackletID1);
				}
				if (!(trackletID2.equals("null"))) {
					reIdAttributesTemp.setTrackletID2(trackletID2);
				}

				reIdAttributesTemp.setSim(sim);
				list.add(reIdAttributesTemp);

			}
			long dbendTime = System.currentTimeMillis();
			System.out.println("Cost everytime of addSimRel of minute : " + (dbendTime - dbstartTime) + "ms");
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		} finally {
			try {
				if (rs!=null) {
					
					rs.close();
					rs=null;
				}
				if (ps!=null) {
					
					ps.close();
					ps=null;
				}
				
			} catch (Exception e2) {
				// TODO: handle exception
				e2.printStackTrace();
			}
			JdbcUtils4Neo4j.releaseConnection(conn);
		}
		
		return list;
	}
}


//
//import java.io.Serializable;
//import java.sql.Connection;
//import java.sql.PreparedStatement;
//import java.util.Iterator;
//
//import org.apache.log4j.Logger;
//
//import db.JdbcUtils;
//import scala.Tuple3;
///* 环境
// * 1.JDK7
// * 2.Neo4j3.x
// * 3.切换数据库需要删除C:\Users\xxx\.neo4j\known_hosts
// */
//
///* 添加jar包
// * 1.neo4j-jdbc-driver-3.0.1.jar
// */
//
///* 配置文件：jdbc.properties
// * jdbc.username = neo4j
// * jdbc.password = root
// * jdbc.driver   = org.neo4j.jdbc.Driver
// * jdbc.url      = jdbc:neo4j:bolt://localhost
// */
//
//public class Neo4jDaoJdbc implements Serializable{
//	/**
//	 * 
//	 */
//	private static final long serialVersionUID = -5157847407490829460L;
//	private static final Logger log = Logger.getLogger(Neo4jDaoJdbc.class);// 日志文件
//	// 表示定义数据库的用户名
////	private static String USERNAME;
////	// 定义数据库的密码
////	private static String PASSWORD;
////	// 定义数据库的驱动信息
////	private static String DRIVER;
////	// 定义访问数据库的地址
////	private static String URL;
////	// 定义数据库的链接
////	private Connection conn;
//	// 定义sql语句的执行对象
////	private PreparedStatement ps;
//	// 定义查询返回的结果集合
////	private ResultSet rs;
//	//影响行数（数据变更后，影响行数都是大于0，等于0时没变更，所以说如果变更失败，那么影响行数必定为负）  
//    private int row=-1; 
//	
////    static {
////		// 加载数据库配置信息，并给相关的属性赋值
////		loadConfig();
////	}
//    
//    private static final String min_sql = "MATCH (a:Person {trackletID: {1}}), (b:Person {trackletID: {2}}) "
//			+ "MERGE (a)-[r:Similarity]-(b) set r.Minute={3}";
//    private static final String hour_sql = "MATCH (a:Person {trackletID: {1}}), (b:Person {trackletID: {2}}) "
//    		+ "MERGE (a)-[r:Similarity]-(b) set r.Hour={3}";
//
//	/**
//	 * 加载数据库配置信息，并给相关的属性赋值
//	 */
////	public void loadConfig() {
////		try {
////			InputStream inStream = Neo4jDaoJdbc.class.getResourceAsStream("/jdbc.properties");
////			Properties prop = new Properties();
////			prop.load(inStream);
////			USERNAME = prop.getProperty("jdbc.username");
////			PASSWORD = prop.getProperty("jdbc.password");
////			DRIVER = prop.getProperty("jdbc.driver");
////			URL = prop.getProperty("jdbc.url");
//////			System.out.println(USERNAME+","+PASSWORD+","+DRIVER+","+URL);
////		} catch (Exception e) {
////			throw new RuntimeException("读取数据库配置文件异常！", e);
////		}
////	}
//
//	public Neo4jDaoJdbc() {
////		Neo4jDaoJdbc.loadConfig();
//	}
//
//	/**
//	 * 获取数据库连接
//	 */
////	public Connection getConnection() {
//////		Connection conn = null;
////		try {
////			// 加载驱动特殊处理，否则找不到驱动包
////			//.newInstance()
////			Class.forName(DRIVER);
////			// 注册驱动
////			conn = DriverManager.getConnection(URL, USERNAME, PASSWORD); // 获取连接
////		} catch (Exception e) {
////			e.printStackTrace();
////		} 
////		log.info("获取连接成功！");
////		return conn;
////	}
//
//	/**
//	 * 执行更新操作
//	 */
////	public int update(String cql, Object... params) throws SQLException {
////		System.out.println(cql + ":::" + Arrays.toString(params));
////		log.info(cql + ":::" + Arrays.toString(params));
////		ps = conn.prepareStatement(cql);
////		int index = 1;
////		// 填充sql语句中的占位符
////		if (params != null && params.length > 0) {
////			for (int i = 0; i < params.length; i++) {
////				ps.setObject(index++, params[i]);
////			}
////		}
////		return ps.executeUpdate();
////	}
//
//	/**
//	 * 执行查询操作
//	 */
////	public List<Map<String, Object>> findList(String cql, Object... params) throws SQLException {
////		//System.out.println(cql + ":::" + Arrays.toString(params));
////		//log.info(cql + ":::" + Arrays.toString(params));
////		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
////		int index = 1;
////		ps = conn.prepareStatement(cql);
////		if (params != null && params.length > 0) {
////			for (int i = 0; i < params.length; i++) {
////				ps.setObject(index++, params[i]);
////			}
////		}
////		long dbstartTime = System.currentTimeMillis();
////		rs = ps.executeQuery();
////		long dbendTime = System.currentTimeMillis();
////		System.out.println("Cost time of db: " + (dbendTime - dbstartTime) + "ms");
////		ResultSetMetaData metaData = rs.getMetaData();
////		int cols_len = metaData.getColumnCount();
////		while (rs.next()) {
////			Map<String, Object> map = new HashMap<String, Object>();
////			for (int i = 0; i < cols_len; i++) {
////				String cols_name = metaData.getColumnName(i + 1);
////				Object cols_value = rs.getObject(cols_name);
////				if (cols_value == null) {
////					cols_value = "";
////				}
////				map.put(cols_name, cols_value);
////			}
////			list.add(map);
////		}
////		return list;
////	}
//
//	/**
//	 * 释放资源
//	 */
//	public void close() {
////		if (rs != null) {
////			try {
////				rs.close();
////			} catch (SQLException e) {
////				e.printStackTrace();
////			}
////		}
////		if (ps != null) {
////			try {
////				ps.close();
////			} catch (SQLException e) {
////				e.printStackTrace();
////			}
////		}
////		if (conn != null) {
////			try {
////				conn.close();
////			} catch (SQLException e) {
////				e.printStackTrace();
////			}
////		}
//	}
//
//	public static void main(String[] args)  {
////		Neo4jDaoJdbc neo4jDaoJdbc = new Neo4jDaoJdbc();
////		neo4jDaoJdbc.loadConfig();
////		neo4jDaoJdbc.getConnection();
////		try {
//////			neo4jDaoJdbc.copyNodes();
////		} catch (Exception e) {
////			// TODO: handle exception
////		}finally {
////			neo4jDaoJdbc.close();
////			
////		}
//		// 查询点
////		String sql1 = "MATCH (n) RETURN n LIMIT {1} ";
////		// 查询线
////		String sql2 = "MATCH ()-[r]->() RETURN r LIMIT {1} ";
////		// 查询所有
////		String sql3 = "MATCH (a)-[r]->(b) RETURN a,b,r LIMIT {1} ";
////		
////		String sql4="MATCH (a:Person {trackletID: 'CAM01-20140226113013-20140226113601_tarid55'})"
////				+ ", (b:Person {trackletID: 'CAM01-20140226113013-20140226113601_tarid54'})"
////				+ " MERGE (a)-[r:Similarity]->(b) set r.Minute=1 RETURN a  LIMIT {1} ";
////		try {
////			long dbstartTime = System.currentTimeMillis();
////			List<Map<String, Object>> result = neo4jDaoJdbc.findList(sql4, 1);
////			long dbendTime = System.currentTimeMillis();
//////			System.out.println("Cost time of db: " + (dbendTime - dbstartTime) + "ms");
////			for (Map<String, Object> m : result) {
////				System.out.println(m);
////			}
////		} catch (SQLException e) {
////			e.printStackTrace();
////		} finally {
////			if (null != neo4jDaoJdbc) {
////				neo4jDaoJdbc.close();
////			}
////		}
////		System.out.println("OVER!");
//	}
//	/*
//	public void copyNodes() {
//		List<ReIdAttributesTemp> list = new ArrayList<>();
//		List<ReIdAttributesTemp> list2 = new ArrayList<>();
//		List<ReIdAttributesTemp> list3 = new ArrayList<>();
//		String sql="MATCH (c:Person{dataType:'lijun20170927'})  "
//							+ "where c.reidFeature is not null return c.trackletID,c.reidFeature,c.dataType;";
//		PreparedStatement ps=null;
//		ResultSet rs =null;
//		try {
//			ps = conn.prepareStatement(sql);
//			rs = ps.executeQuery();
//		} catch (SQLException e1) {
//			// TODO Auto-generated catch block
//			e1.printStackTrace();
//		}
//		
//            try {  
//            	if(rs!=null){  
//                    while(rs.next()){  
//                        //遍历每行元素的内容  
//                    	ReIdAttributesTemp reIdAttributesTemp = new ReIdAttributesTemp();
//        				String trackletID = rs.getString(1);
//        				String featureBase64Str = rs.getString(2);
//        				String dataType = rs.getString(3);
//        				if (!featureBase64Str.equals("null")) {
//        					byte[] featureBytes = Base64.decodeBase64(featureBase64Str);
//        					Feature feature = new FeatureMSCAN(featureBytes);
//        					float[] vector=feature.getVector();
//        					
//        					reIdAttributesTemp.setFeatureVector(vector);
//        				}
//        				if (!trackletID.equals("null")) {
//        					reIdAttributesTemp.setTrackletID(trackletID);
//        				}
//        				reIdAttributesTemp.setDataType(dataType);
//        				list.add(reIdAttributesTemp);  
//                    }  
//            	}else{  
//            		System.out.println("结果集不存在！");  
//            	}
//            } catch (SQLException e) {  
//                e.printStackTrace();  
//            }finally {
//            	try {
//					rs.close();
//					ps.close();
//				} catch (SQLException e) {
//					e.printStackTrace();
//				}
//    		}  
//		
//		//打印list
////            for (ReIdAttributesTemp reIdAttributesTemp : list) {
////    			System.out.println(reIdAttributesTemp.getTrackletID()+","+reIdAttributesTemp.getFeatureVector().length+","+reIdAttributesTemp.getFeatureVector()[0]);
////    		}
////            System.out.println("-----------------------------");
//		//copy
//		for (int i = 0; i < 10; i++) {
//			list2.addAll(list);
//		}
//		list.clear();
////		for (ReIdAttributesTemp reIdAttributesTemp : list2) {
////			System.out.println(reIdAttributesTemp.getTrackletID()+","+reIdAttributesTemp.getFeatureVector().length+","+reIdAttributesTemp.getFeatureVector()[0]);
////		}
////		System.out.println("-----------------------------");
//		int len=list2.size();
//		System.out.println("list2:"+len);
//		int k=1;
//		for (int i = 0; i < len; i++) {
//			ReIdAttributesTemp reIdAttributesTempOut=new ReIdAttributesTemp();
//			ReIdAttributesTemp reIdAttributesTemp =list2.get(i);
//			String idTemp=reIdAttributesTemp.getTrackletID()+i;
//			reIdAttributesTempOut.setTrackletID(idTemp);;
//			float[] vector=reIdAttributesTemp.getFeatureVector();
//			float[] outvector=new float[vector.length];
//			for (int j = 0; j < vector.length; j++) {
//				outvector[j]=vector[j]+1;
//			}
//			reIdAttributesTempOut.setFeatureVector(outvector);
//			reIdAttributesTempOut.setDataType(reIdAttributesTemp.getDataType());
//			list3.add(reIdAttributesTempOut);
//		}
////		System.out.println("-------------------------");
////		for (ReIdAttributesTemp reIdAttributesTemp : list3) {
////			System.out.println(reIdAttributesTemp.getTrackletID()+","+reIdAttributesTemp.getFeatureVector().length+","+reIdAttributesTemp.getFeatureVector()[0]);
////		}
//		int length=list3.size();
//		System.out.println("list3:"+length);
//		System.out.println("-------------------------");
//		
//		list2.clear();
//		
//		String outSql="CREATE (c:Person) set c.trackletID={1},c.reidFeature={2},c.dataType={3} ;";
////				tx.run("MATCH (a:Minute {start: {start}}), (b:Person {trackletID: {trackletID}}) MERGE (a)-[:INCLUDES_PERSON]-(b);"
////		                ,Values.parameters("start", start, "trackletID", trackletID));
//		PreparedStatement psOut =null;
//		try {
//			psOut = conn.prepareStatement(outSql);
//			
//			for (int j = 0; j < k; j++) {
//				for (int i = j*(length/k); i < (j+1)*(length/k); i++) {
//					ReIdAttributesTemp reIdAttributesTemp =list3.get(i);
//					String trackletID = reIdAttributesTemp.getTrackletID();
//					String dataType=reIdAttributesTemp.getDataType();
//					float[] featureVector=reIdAttributesTemp.getFeatureVector();
//					Feature feature = new FeatureMSCAN(featureVector);
//					byte[] featureBytes =feature.getBytes();
//					String featureBase64Str = Base64.encodeBase64String(featureBytes);
//					psOut.setString(1, trackletID);
//					psOut.setString(2, featureBase64Str);
//					psOut.setString(3, dataType);
//					psOut.addBatch();
//				}
//				int[] count=psOut.executeBatch();
//				log.info("执行成功:"+j+"-----:-------"+count.length);
//				psOut.clearBatch();
//			}
//		} catch (Exception e) {
//			// TODO: handle exception
//			log.info(e);
//		}finally {
//		try {
//					psOut.close();
//				} catch (SQLException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
//		}
//	}
//	*/
//	/*
//	public List<ReIdAttributesTemp> getPedestrianReIDFeatureList(){
//		List<ReIdAttributesTemp> list = new ArrayList<>();
//		String sql="MATCH (c:Person)  "
//							+ "where c.reidFeature is not null return c.trackletID,c.reidFeature limit 100;";
//		PreparedStatement ps=null;
//		ResultSet rs =null;
//		try {
//			ps = conn.prepareStatement(sql);
//			rs = ps.executeQuery();
//		} catch (SQLException e1) {
//			// TODO Auto-generated catch block
//			e1.printStackTrace();
//		}
//		
//            try {  
//            	if(rs!=null){  
//                    while(rs.next()){  
//                        //遍历每行元素的内容  
//                    	ReIdAttributesTemp reIdAttributesTemp = new ReIdAttributesTemp();
//        				String trackletID = rs.getString(1);
//        				String featureBase64Str = rs.getString(2);
//        				if (!featureBase64Str.equals("null")) {
//        					byte[] featureBytes = Base64.decodeBase64(featureBase64Str);
//        					Feature feature = new FeatureMSCAN(featureBytes);
//        					float[] vector=feature.getVector();
//        					reIdAttributesTemp.setFeatureVector(vector);
//        				}
//        				if (!trackletID.equals("null")) {
//        					reIdAttributesTemp.setTrackletID(trackletID);
//        				}
//        				list.add(reIdAttributesTemp);  
//                    }
//            	}else{  
//            		System.out.println("结果集不存在！");  
//            	}
//            } catch (SQLException e) {  
//                e.printStackTrace();  
//            }finally {
//            	try {
//					rs.close();
//					ps.close();
//				} catch (SQLException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
//    		}
//			return list;
//		
//	}
//	*/
//	public int addSimRel(
////			List<Tuple3<String, scala.Double, String>> list
//			Iterator<scala.collection.immutable.List<Tuple3<String, scala.Double, String>>> iterator
////			,Connection conn
//			,String level
//			) {
////		loadConfig();
////		Connection conn = DBUtil.getConnection();
////		Connection conn = JdbcUtils.getConnection();  
//		long dbstartTime = System.currentTimeMillis();
//		PreparedStatement ps = null;
////		ResultSet rs = null;
////		System.out.println("jdbc addSimrel");
////		List<ReIdAttributesTemp> outlist = new ArrayList<>();
////		int[] count =null;
//		int countAll=0;
//		int num=0;
//		try {
//			if (iterator != null) {
//				while (iterator.hasNext()) {
//					scala.collection.immutable.List<Tuple3<String, scala.Double, String>> list = iterator.next();
//					if (list != null) {
//						// System.out.println("该次保存的list的length是：" +
//						// list.length());
//						//
//						if (list.size() > 0 || list.length() > 0) {
//							System.out.println("该次需要保存的list的大小是：" + list.size() + "," + list.length());
//							try {
////								JdbcUtils.beginTransaction();
//								Connection conn =null;
//								// conn.setAutoCommit(false); // 关闭自动提交事务（开启事务）
//
////								if (conn == null) {
////									conn = DBUtil.getConnection();
////									ps = conn.prepareStatement(sql);
////								} else {
//								conn = JdbcUtils.getConnection();
//								if (level.equals("minute")) {
//									
//									ps = conn.prepareStatement(min_sql);
//								}if (level.equals("hour")) {
//									
//									ps = conn.prepareStatement(hour_sql);
//								}
////								}
//								for (int i = 0; i < list.size(); i++) {
//									// Tuple3<String, scala.Double, String>
//									// tuple = list.get(i);
//									Tuple3<String, scala.Double, String> tuple = list.apply(i);
//									String nodeID1 = tuple._1();
//									String nodeID2 = tuple._3();
//									double SimRel = java.lang.Double.valueOf(tuple._2() + "");
//									System.out.println("jdbc min需要保存的结果是：[{'sim':" + SimRel + ",'trackletID1':'"
//											+ nodeID1 + "','trackletID2':'" + nodeID2 + "'}]");
//									ps.setString(1, nodeID1);
//									ps.setString(2, nodeID2);
//									ps.setDouble(3, SimRel);
//									ps.addBatch();
//								}
//								int[] count = ps.executeBatch();
//								// conn.commit();
//								System.out.println("执行成功的个数是:" + count.length);
//								countAll += count.length;
//								// rs = ps.executeQuery();
//								ps.clearBatch();
//								if (ps != null) {
//
//									ps.close();
//								} 
////									else {
////									System.out.println("ps 是null");
////								}
//								JdbcUtils.releaseConnection(conn);
////								JdbcUtils.commitTransaction();
//							} catch (Exception e) {
//								// TODO: handle exception
////								e.printStackTrace();
//							}
//							// try {
//							// if (rs != null) {
//							// while (rs.next()) {
//							// // 遍历每行元素的内容
//							// ReIdAttributesTemp reIdAttributesTemp = new
//							// ReIdAttributesTemp();
//							// String trackletID1 = rs.getString(1);
//							// String trackletID2 = rs.getString(2);
//							// java.lang.Double sim = rs.getDouble(3);
//							// if (!(trackletID1.equals("null"))) {
//							// reIdAttributesTemp.setTrackletID1(trackletID1);
//							// }
//							// if (!(trackletID2.equals("null"))) {
//							// reIdAttributesTemp.setTrackletID2(trackletID2);
//							// }
//							//
//							// reIdAttributesTemp.setSim(sim);
//							// outlist.add(reIdAttributesTemp);
//							// }
//							// } else {
//							// System.out.println("结果集不存在！");
//							// }
//							// } catch (SQLException e) {
//							// e.printStackTrace();
//							// }
//							finally {
//								try {
//									// rs.close();
//									
////									JdbcUtils.rollbackTransaction();
//									// if (conn!=null) {
//									// DBUtil.closeConnection();
//									// }else {
//									// System.out.println("conn 是null");
//									// }
//									// conn.close();
//								} catch (Exception e) {
//									// TODO Auto-generated catch block
////									e.printStackTrace();
//								}
//							}
//
//						} else {
//							System.out.println("Iterator list 的大小是0");
//						}
//					} else {
//						System.out.println("Iterator list is null");
//					}
//				num++;
//				}
//			} else {
//				System.out.println("Iterator is null");
//			}
//		} catch (Exception e) {
//			// TODO: handle exception
////			e.printStackTrace();
//		} finally {
//			// TODO: handle finally clause
//			try {
//				// rs.close();
//				// ps.close();
////				if (conn != null) {
//////					DBUtil.closeConnection();
////					conn.close();
////				} else {
////					System.out.println("conn 是null");
////				}
//			} catch (Exception e) {
//				// TODO Auto-generated catch block
////				e.printStackTrace();
//			}
//		}
//		long dbendTime = System.currentTimeMillis();
////		if (count != null) {
////			if (countAll != 0) {
////			System.out.println("该次需要保存的list的size是：+ list.size()"  + "," + "执行成功的个数是:" + count.length + ","
////					+ "Cost batch everytime of addSimRel of minute : " + (dbendTime - dbstartTime) + "ms");
////
////		} else {
////			System.out.println("执行没有成功，count是null");
////		}
//		System.out.println("循环了多少次:"+num+",该次需要保存的数量是："+countAll  + ",Cost everytime of addSimRel of minute : " + (dbendTime - dbstartTime) + "ms");
////		List<ReIdAttributesTemp> outlist = new ArrayList<>();
//		/*String Outsql = "MATCH (a:Person{trackletID: {1}})-[r:Similarity]-(b:Person{trackletID: {2}}) return a.trackletID,b.trackletID,r.Minute";
//		PreparedStatement psOut = null;
//		ResultSet rs = null;
//		try {
//			psOut = conn.prepareStatement(Outsql);
//			rs = psOut.executeQuery();
//		} catch (SQLException e1) {
//			// TODO Auto-generated catch block
//			e1.printStackTrace();
//		}
//		try {
//			if (rs != null) {
//				while (rs.next()) {
//					// 遍历每行元素的内容
//					ReIdAttributesTemp reIdAttributesTemp = new ReIdAttributesTemp();
//					String trackletID1 = rs.getString(1);
//					String trackletID2 = rs.getString(2);
//					java.lang.Double sim = rs.getDouble(3);
//					if (!(trackletID1.equals("null"))) {
//						reIdAttributesTemp.setTrackletID1(trackletID1);
//					}
//					if (!(trackletID2.equals("null"))) {
//						reIdAttributesTemp.setTrackletID2(trackletID2);
//					}
//
//					reIdAttributesTemp.setSim(sim);
//					outlist.add(reIdAttributesTemp);
//				}
//			} else {
//				System.out.println("结果集不存在！");
//			}
//		} catch (SQLException e) {
//			e.printStackTrace();
//		} finally {
//			try {
//				rs.close();
//				psOut.close();
//			} catch (SQLException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//		}*/
//		return countAll;
//
//	}
//}
