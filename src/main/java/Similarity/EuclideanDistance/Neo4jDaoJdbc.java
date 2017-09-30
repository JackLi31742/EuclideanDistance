package Similarity.EuclideanDistance;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.Values;

import entities.Feature;
import entities.FeatureMSCAN;
import entities.ReIdAttributesTemp;

/* 环境
 * 1.JDK7
 * 2.Neo4j3.x
 * 3.切换数据库需要删除C:\Users\xxx\.neo4j\known_hosts
 */

/* 添加jar包
 * 1.neo4j-jdbc-driver-3.0.1.jar
 */

/* 配置文件：jdbc.properties
 * jdbc.username = neo4j
 * jdbc.password = root
 * jdbc.driver   = org.neo4j.jdbc.Driver
 * jdbc.url      = jdbc:neo4j:bolt://localhost
 */

public class Neo4jDaoJdbc {
	private static final Logger log = Logger.getLogger(Neo4jDaoJdbc.class);// 日志文件
	// 表示定义数据库的用户名
	private String USERNAME;
	// 定义数据库的密码
	private String PASSWORD;
	// 定义数据库的驱动信息
	private String DRIVER;
	// 定义访问数据库的地址
	private String URL;
	// 定义数据库的链接
	private Connection conn;
	// 定义sql语句的执行对象
//	private PreparedStatement ps;
	// 定义查询返回的结果集合
//	private ResultSet rs;
	//影响行数（数据变更后，影响行数都是大于0，等于0时没变更，所以说如果变更失败，那么影响行数必定为负）  
    private int row=-1; 
	
//    static {
//		// 加载数据库配置信息，并给相关的属性赋值
//		loadConfig();
//	}

	/**
	 * 加载数据库配置信息，并给相关的属性赋值
	 */
	public void loadConfig() {
		try {
			InputStream inStream = Neo4jDaoJdbc.class.getResourceAsStream("/jdbc.properties");
			Properties prop = new Properties();
			prop.load(inStream);
			USERNAME = prop.getProperty("jdbc.username");
			PASSWORD = prop.getProperty("jdbc.password");
			DRIVER = prop.getProperty("jdbc.driver");
			URL = prop.getProperty("jdbc.url");
		} catch (Exception e) {
			throw new RuntimeException("读取数据库配置文件异常！", e);
		}
	}

	public Neo4jDaoJdbc() {
	}

	/**
	 * 获取数据库连接
	 */
	public Connection getConnection() {
		try {
			// 加载驱动特殊处理，否则找不到驱动包
			Class.forName(DRIVER).newInstance();
			// 注册驱动
			conn = DriverManager.getConnection(URL, USERNAME, PASSWORD); // 获取连接
		} catch (Exception e) {
			e.printStackTrace();
		} 
		log.info("获取连接成功！");
		return conn;
	}

	/**
	 * 执行更新操作
	 */
//	public int update(String cql, Object... params) throws SQLException {
//		System.out.println(cql + ":::" + Arrays.toString(params));
//		log.info(cql + ":::" + Arrays.toString(params));
//		ps = conn.prepareStatement(cql);
//		int index = 1;
//		// 填充sql语句中的占位符
//		if (params != null && params.length > 0) {
//			for (int i = 0; i < params.length; i++) {
//				ps.setObject(index++, params[i]);
//			}
//		}
//		return ps.executeUpdate();
//	}

	/**
	 * 执行查询操作
	 */
//	public List<Map<String, Object>> findList(String cql, Object... params) throws SQLException {
//		//System.out.println(cql + ":::" + Arrays.toString(params));
//		//log.info(cql + ":::" + Arrays.toString(params));
//		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
//		int index = 1;
//		ps = conn.prepareStatement(cql);
//		if (params != null && params.length > 0) {
//			for (int i = 0; i < params.length; i++) {
//				ps.setObject(index++, params[i]);
//			}
//		}
//		long dbstartTime = System.currentTimeMillis();
//		rs = ps.executeQuery();
//		long dbendTime = System.currentTimeMillis();
//		System.out.println("Cost time of db: " + (dbendTime - dbstartTime) + "ms");
//		ResultSetMetaData metaData = rs.getMetaData();
//		int cols_len = metaData.getColumnCount();
//		while (rs.next()) {
//			Map<String, Object> map = new HashMap<String, Object>();
//			for (int i = 0; i < cols_len; i++) {
//				String cols_name = metaData.getColumnName(i + 1);
//				Object cols_value = rs.getObject(cols_name);
//				if (cols_value == null) {
//					cols_value = "";
//				}
//				map.put(cols_name, cols_value);
//			}
//			list.add(map);
//		}
//		return list;
//	}

	/**
	 * 释放资源
	 */
	public void close() {
//		if (rs != null) {
//			try {
//				rs.close();
//			} catch (SQLException e) {
//				e.printStackTrace();
//			}
//		}
//		if (ps != null) {
//			try {
//				ps.close();
//			} catch (SQLException e) {
//				e.printStackTrace();
//			}
//		}
		if (conn != null) {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args)  {
		Neo4jDaoJdbc neo4jDaoJdbc = new Neo4jDaoJdbc();
		neo4jDaoJdbc.loadConfig();
		neo4jDaoJdbc.getConnection();
		try {
			neo4jDaoJdbc.copyNodes();
		} catch (Exception e) {
			// TODO: handle exception
		}finally {
			neo4jDaoJdbc.close();
			
		}
		// 查询点
//		String sql1 = "MATCH (n) RETURN n LIMIT {1} ";
//		// 查询线
//		String sql2 = "MATCH ()-[r]->() RETURN r LIMIT {1} ";
//		// 查询所有
//		String sql3 = "MATCH (a)-[r]->(b) RETURN a,b,r LIMIT {1} ";
//		
//		String sql4="MATCH (a:Person {trackletID: 'CAM01-20140226113013-20140226113601_tarid55'})"
//				+ ", (b:Person {trackletID: 'CAM01-20140226113013-20140226113601_tarid54'})"
//				+ " MERGE (a)-[r:Similarity]->(b) set r.Minute=1 RETURN a  LIMIT {1} ";
//		try {
//			long dbstartTime = System.currentTimeMillis();
//			List<Map<String, Object>> result = neo4jDaoJdbc.findList(sql4, 1);
//			long dbendTime = System.currentTimeMillis();
////			System.out.println("Cost time of db: " + (dbendTime - dbstartTime) + "ms");
//			for (Map<String, Object> m : result) {
//				System.out.println(m);
//			}
//		} catch (SQLException e) {
//			e.printStackTrace();
//		} finally {
//			if (null != neo4jDaoJdbc) {
//				neo4jDaoJdbc.close();
//			}
//		}
//		System.out.println("OVER!");
	}
	
	public void copyNodes() {
		List<ReIdAttributesTemp> list = new ArrayList<>();
		List<ReIdAttributesTemp> list2 = new ArrayList<>();
		String sql="MATCH (c:Person)  "
							+ "where c.reidFeature is not null return c.trackletID,c.reidFeature ;";
		PreparedStatement ps=null;
		ResultSet rs =null;
		try {
			ps = conn.prepareStatement(sql);
			rs = ps.executeQuery();
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
            try {  
            	if(rs!=null){  
                    while(rs.next()){  
                        //遍历每行元素的内容  
                    	ReIdAttributesTemp reIdAttributesTemp = new ReIdAttributesTemp();
//        				Long start = rs.getLong(1);
        				String trackletID = rs.getString(1);
        				String featureBase64Str = rs.getString(2);
//        				String dataType = rs.getString(4);
        				if (!featureBase64Str.equals("null")) {
        					byte[] featureBytes = Base64.decodeBase64(featureBase64Str);
        					Feature feature = new FeatureMSCAN(featureBytes);
        					float[] vector=feature.getVector();
        					
        					reIdAttributesTemp.setFeatureVector(vector);
        				}
        				if (!trackletID.equals("null")) {
        					reIdAttributesTemp.setTrackletID(trackletID);
        				}
//        				reIdAttributesTemp.setStart(start);
//        				reIdAttributesTemp.setDataType(dataType);
        				list.add(reIdAttributesTemp);  
                    }  
            	}else{  
            		System.out.println("结果集不存在！");  
            	}
            } catch (SQLException e) {  
                e.printStackTrace();  
            }finally {
            	try {
					rs.close();
					ps.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
    		}  
		
		//打印list
		/*for (ReIdAttributesTemp reIdAttributesTemp : list) {
			System.out.println(reIdAttributesTemp.getTrackletID()+","+reIdAttributesTemp.getFeatureVector().length);
		}*/
		//copy
		for (int i = 0; i < 10000; i++) {
			list2.addAll(list);
		}
		int len=list2.size();
		log.info("len:"+len);
		int k=1000;
		for (int i = 0; i < len; i++) {
			ReIdAttributesTemp reIdAttributesTemp =list2.get(i);
			String idTemp=reIdAttributesTemp.getTrackletID()+i;
			reIdAttributesTemp.setTrackletID(idTemp);;
			float[] vector=reIdAttributesTemp.getFeatureVector();
			float[] outvector=new float[vector.length];
			for (int j = 0; j < vector.length; j++) {
				outvector[j]=vector[j]+1;
			}
			reIdAttributesTemp.setFeatureVector(outvector);
		}
		System.out.println("-------------------------");
		/*for (ReIdAttributesTemp reIdAttributesTemp : list2) {
			System.out.println(reIdAttributesTemp.getTrackletID()+","+reIdAttributesTemp.getFeatureVector().length);
		}*/
		System.out.println("-------------------------");
		String outSql="CREATE (c:Person) set c.trackletID={1},c.reidFeature={2} ;";
//				tx.run("MATCH (a:Minute {start: {start}}), (b:Person {trackletID: {trackletID}}) MERGE (a)-[:INCLUDES_PERSON]-(b);"
//		                ,Values.parameters("start", start, "trackletID", trackletID));
		PreparedStatement psOut =null;
		try {
			psOut = conn.prepareStatement(outSql);
			
			for (int j = 0; j < k; j++) {
				for (int i = j*(len/k); i < (j+1)*(len/k); i++) {
					ReIdAttributesTemp reIdAttributesTemp =list2.get(i);
					String trackletID = reIdAttributesTemp.getTrackletID();
	//				Long start=reIdAttributesTemp.getStart();
	//				String dataType=reIdAttributesTemp.getDataType();
					float[] featureVector=reIdAttributesTemp.getFeatureVector();
					Feature feature = new FeatureMSCAN(featureVector);
					byte[] featureBytes =feature.getBytes();
					String featureBase64Str = Base64.encodeBase64String(featureBytes);
					psOut.setString(1, trackletID);
	//				psOut.setString(2, dataType);
					psOut.setString(2, featureBase64Str);
					psOut.addBatch();
				}
				int[] count=psOut.executeBatch();
				log.info("执行成功:"+j+","+count.length);
				psOut.clearBatch();
			}
		} catch (Exception e) {
			// TODO: handle exception
			log.info(e);
		}finally {
		try {
					rs.close();
					ps.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
	}
}
