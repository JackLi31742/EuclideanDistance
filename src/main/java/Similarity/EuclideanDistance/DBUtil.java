package Similarity.EuclideanDistance;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;
import java.util.Vector;

import org.apache.log4j.Logger;

public class DBUtil implements Serializable{

	private static final long serialVersionUID = 8073283516346263684L;
	private static final Logger log = Logger.getLogger(DBUtil.class);// 日志文件
	// 表示定义数据库的用户名
	private static final String USERNAME="neo4j";
	// 定义数据库的密码
	private static final String PASSWORD="casia@1234";
	// 定义数据库的驱动信息
	private static final String DRIVER="org.neo4j.jdbc.Driver";
	// 定义访问数据库的地址
	private static final String URL="jdbc:neo4j:bolt://172.18.33.37";
	
	// 定义一个用于放置数据库连接的局部线程变量（使每个线程都拥有自己的连接）  
    private static ThreadLocal<Connection> connContainer = new ThreadLocal<Connection>(); 
 // 连接池配置属性  
//    private DBbean dbBean;  
    private boolean isActive = false; // 连接池活动状态  
    private int contActive = 0;// 记录创建的总的连接数  
      
    // 空闲连接  
    private List<Connection> freeConnection = new Vector<Connection>();  
    // 活动连接  
    private List<Connection> activeConnection = new Vector<Connection>();  
  
	
	/**
	 * 加载数据库配置信息，并给相关的属性赋值
	 */
	public static void loadConfig() {
//		try {
//			InputStream inStream = Neo4jDaoJdbc.class.getResourceAsStream("/jdbc.properties");
//			Properties prop = new Properties();
//			prop.load(inStream);
//			USERNAME = prop.getProperty("jdbc.username");
//			PASSWORD = prop.getProperty("jdbc.password");
//			DRIVER = prop.getProperty("jdbc.driver");
//			URL = prop.getProperty("jdbc.url");
////			System.out.println(USERNAME+","+PASSWORD+","+DRIVER+","+URL);
//		} catch (Exception e) {
//			throw new RuntimeException("读取数据库配置文件异常！", e);
//		}
	}
	
	// 获取连接  
    public synchronized static Connection getConnection() {
//    	loadConfig();
//		System.out.println(USERNAME+","+PASSWORD+","+DRIVER+","+URL);
        Connection conn = connContainer.get();  
        try {  
            if (conn == null) {
            	//加载驱动至内存 
            	Class.forName(DRIVER);
    			//获取连接
    			conn = DriverManager.getConnection(URL, USERNAME, PASSWORD); 
            }  
        } catch (Exception e) {  
        	e.printStackTrace();  
        } finally {  
            connContainer.set(conn);  
        }  
        System.out.println("获取连接成功！");
        return conn;  
    }  
  
    // 关闭连接  
    public synchronized static void closeConnection() {  
        Connection conn = connContainer.get();  
        try {  
            if (conn != null) {  
                conn.close();  
            }  
        } catch (Exception e) {  
            e.printStackTrace();  
        } finally {  
            connContainer.remove();  
        }  
    }  
}  


