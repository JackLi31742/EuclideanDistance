package entities;

import org.bytedeco.javacpp.FloatPointer;
import org.bytedeco.javacpp.opencv_core.Mat;

public class FeatureData {

	private int arr1Row;
	private int arr2Row;
	
	private Mat mat1 ;
	private FloatPointer fp1 ;
	private Mat mat2;
    private FloatPointer fp2 ;
    
    private Mat indexMat;
    private Mat distsMat;
   
    private String[] trackletID1s;
    private String[] trackletID2s;
    private String trackletID1;
    private String trackletID2;
    
    
	public String[] getTrackletID2s() {
		return trackletID2s;
	}
	public void setTrackletID2s(String[] trackletID2s) {
		this.trackletID2s = trackletID2s;
	}
	public String[] getTrackletID1s() {
		return trackletID1s;
	}
	public void setTrackletID1s(String[] trackletID1s) {
		this.trackletID1s = trackletID1s;
	}
	public String getTrackletID1() {
		return trackletID1;
	}
	public void setTrackletID1(String trackletID1) {
		this.trackletID1 = trackletID1;
	}
	public String getTrackletID2() {
		return trackletID2;
	}
	public void setTrackletID2(String trackletID2) {
		this.trackletID2 = trackletID2;
	}
	public Mat getIndexMat() {
		return indexMat;
	}
	public void setIndexMat(Mat indexMat) {
		this.indexMat = indexMat;
	}
	public Mat getDistsMat() {
		return distsMat;
	}
	public void setDistsMat(Mat distsMat) {
		this.distsMat = distsMat;
	}
	public int getArr1Row() {
		return arr1Row;
	}
	public void setArr1Row(int arr1Row) {
		this.arr1Row = arr1Row;
	}
	public int getArr2Row() {
		return arr2Row;
	}
	public void setArr2Row(int arr2Row) {
		this.arr2Row = arr2Row;
	}
	public Mat getMat1() {
		return mat1;
	}
	public void setMat1(Mat mat1) {
		this.mat1 = mat1;
	}
	public FloatPointer getFp1() {
		return fp1;
	}
	public void setFp1(FloatPointer fp1) {
		this.fp1 = fp1;
	}
	public Mat getMat2() {
		return mat2;
	}
	public void setMat2(Mat mat2) {
		this.mat2 = mat2;
	}
	public FloatPointer getFp2() {
		return fp2;
	}
	public void setFp2(FloatPointer fp2) {
		this.fp2 = fp2;
	}
    
    
}
