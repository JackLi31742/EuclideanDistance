package deletedemo;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import Similarity.EuclideanDistance.Neo4jConnector;
import entities.ReIdAttributesTemp;
/**
 * 删除误插入的边
 * @author LANG
 *
 */
public class DeleteDemo {

	public static void main(String[] args) throws IOException {
		FileReader fr=new FileReader("E:\\test\\1.txt");
		BufferedReader bufr=new BufferedReader(fr);
		String line="";
		//一行一行读比较方便
		List<ReIdAttributesTemp> list=new ArrayList<>();
		Neo4jConnector dbConnector=new Neo4jConnector();
		while((line=bufr.readLine())!=null){
			if (line.contains("min需要保存的结果是：")) {
				ReIdAttributesTemp reIdAttributesTemp=new ReIdAttributesTemp();
				String temp=line.split("min需要保存的结果是：")[1];
//				System.out.println(temp);
				String []tempArr=temp.split(",");
				String sim=tempArr[0].split(":")[1];
				String id1=tempArr[1].split(":")[1].split("'")[1].split("'")[0];
				String id2=tempArr[2].split(":")[1].split("'")[1].split("'")[0];
				System.out.println("sim:"+sim+",id1:"+id1+",id2:"+id2);
				dbConnector.delete(id1, id2);
				System.out.println("-----------------");
			}
		}
		bufr.close();
	}
}
