package test;

import java.util.ArrayList;
import java.util.List;
import Jama.Matrix;
public class ArrayTest {

	public static void main(String[] args) {
//		double d[][]={{1.0,2.0,3.0,4.0}};
//		Matrix matrix=new Matrix(d);
//		Matrix matrixt=matrix.transpose();
//		
//		for (int i = 0; i < 1; i++) {
//			for (int j = 0; j < 4; j++) {
//				
//				matrix.print(i, j);;
//				System.out.println("----------------");
//			}
//		}
		name();
	}
	
	public static void name() {
		Matrix matrix=new Matrix(3,4);
		Er e[][]=new Er[3][4];
		List<Er> list=new ArrayList<>();
		for (int i = 0; i < 3; i++) {
			for (int j = 0; j < 4; j++) {
				
				Er e1=new Er(i,j);  
				matrix.set(i, j, (i+1)*(j+1));
				list.add(e1);
			}
		}
		
		for (Er[] ers : e) {
			for (Er er : ers) {
			}
		}
		System.out.println(list.size());
		for (Er er : list) {
			System.out.print(er.x+","+er.y);
			System.out.println();
		}
		
//		for (int i = 0; i < 3; i++) {
//			for (int j = 0; j < 4; j++) {
//				
//				matrix.print(i, j);;
//			}
//		}
	}
}


