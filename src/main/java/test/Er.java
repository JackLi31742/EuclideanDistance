package test;

import Similarity.EuclideanDistance.Neo4jConnector;
import Similarity.EuclideanDistance.util.Factory;
public class Er {
	int x;
	int y;
	public Er(int x, int y) {
		super();
		this.x = x;
		this.y = y;
	}
	
	public static void main(String[] args) {
		new Factory<Neo4jConnector>(){

			@Override
			public Neo4jConnector produce() throws Exception {
				// TODO Auto-generated method stub
				return null;
			}
			
		};
	}
}
