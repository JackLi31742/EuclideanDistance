package Similarity.EuclideanDistance;

import static org.bytedeco.javacpp.opencv_core.CV_32FC1;
import static org.bytedeco.javacpp.opencv_core.CV_32SC1;
import static org.bytedeco.javacpp.opencv_flann.EUCLIDEAN;
import static org.bytedeco.javacpp.opencv_flann.FLANN_DIST_EUCLIDEAN;
import static org.bytedeco.javacpp.opencv_flann.FLANN_DIST_HAMMING;
import static org.bytedeco.javacpp.opencv_flann.FLANN_DIST_L2;

import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.ArrayUtils;
import org.bytedeco.javacpp.FloatPointer;
import org.bytedeco.javacpp.IntPointer;
import org.bytedeco.javacpp.opencv_core.Mat;
import org.bytedeco.javacpp.opencv_flann.Index;
import org.bytedeco.javacpp.opencv_flann.IndexParams;
import org.bytedeco.javacpp.opencv_flann.KDTreeIndexParams;
import org.bytedeco.javacpp.opencv_flann.LshIndexParams;

import entities.FeatureData;
import entities.ReIdAttributesTemp;
import scala.Tuple2;
/**
 * Knn Search using flann in JavaCV
 *
 * Version 0.0.1
 * da.li on 2017/09/18
 */
public class JavaKnn 
{
    // Underlying parameters for knnsearch.
	private int k = 1;
	private int method = EUCLIDEAN;
	
    private Index flannIndex = null;
    private IndexParams indexParams = null;
//    private SearchParams searchParams = null;
    private Mat indexMat, distMat;
    
    

    // Initialization.
    public void init(int k, int method) {
        this.k = k;
        this.method = method;
        // Params for searching ...
        flannIndex = new Index();
        // TODO: other methods ...
        if (method == FLANN_DIST_EUCLIDEAN ||
            method == EUCLIDEAN ||
            method == FLANN_DIST_L2) {
            indexParams = new KDTreeIndexParams(4);  // default params = 4
        } else if (method == FLANN_DIST_HAMMING) {
            indexParams = new LshIndexParams(12, 20, 2); // using LSH Hamming distance (default params)
        } else {
            System.out.println("Bad method, use KD Tree instead!");
            indexParams = new KDTreeIndexParams(4);
        }
//        searchParams = new SearchParams(128, 0, true); // maximum number of leafs checked.
//        searchParams = new SearchParams(); // maximum number of leafs checked.
    }

    // Knn search.
    public void knnSearch(Mat probes, Mat gallery) {
        int rows = probes.rows();
        indexMat = new Mat(rows, k, CV_32SC1);
        distMat = new Mat(rows, k, CV_32FC1);
        // find nearest neighbors using FLANN
        // TODO: If it can be built only once?
        flannIndex.build(gallery, indexParams, method);
//        flannIndex.knnSearch(probes, indexMat, distMat, k, searchParams);
        flannIndex.knnSearch(probes, indexMat, distMat, k);
//        System.out.println("knnSearch test");
    }
    public void knnSearch(FeatureData fd) {
        int rows =fd.getArr2Row();
        indexMat = new Mat(rows, k, CV_32SC1);
        distMat = new Mat(rows, k, CV_32FC1);
        // find nearest neighbors using FLANN
        // TODO: If it can be built only once?
        flannIndex.build(fd.getMat1(), indexParams, method);
//        flannIndex.knnSearch(probes, indexMat, distMat, k, searchParams);
        flannIndex.knnSearch(fd.getMat2(), indexMat, distMat, k);

    }
    // Get knn results
    // Index matrix.
    public Mat getIndexMat() {
        return this.indexMat;
    }
    // Distance matrix.
    public Mat getDistMat() {
        return this.distMat;
    }
    
    public FeatureData getMats(float[] arr1,float[] arr2,int col) {
    	int arr1len=arr1.length;
    	int arr2len=arr2.length;
//    	System.out.println("arr1len:"+arr1len+",arr2len:"+arr2len);
    	int arr1Row=arr1len/col;
    	int arr2Row=arr2len/col;
    	
    	//得到mat
    	Mat mat1 = new Mat(arr1Row, col, CV_32FC1);
    	FloatPointer fp1 = new FloatPointer(mat1.data());
        fp1.put(arr1);
        						//row //col
        Mat mat2 = new Mat(arr2Row, col, CV_32FC1);
        FloatPointer fp2 = new FloatPointer(mat2.data());
        fp2.put(arr2);
        
        FeatureData fd=new FeatureData();
        fd.setArr1Row(arr1Row);
        fd.setArr2Row(arr2Row);
        fd.setFp1(fp1);
        fd.setFp2(fp2);
        fd.setMat1(mat1);
        fd.setMat2(mat2);
        
        return fd;
	}

    public List<ReIdAttributesTemp> getResults(FeatureData fd,int col) {
    	Mat mat1=fd.getMat1();
    	Mat mat2=fd.getMat2();
    	int arr1Row=fd.getArr1Row();
    	int arr2Row=fd.getArr2Row();
    	Mat indexMat=fd.getIndexMat();
    	Mat distsMat=fd.getDistsMat();
    	String[] trackletID1s=fd.getTrackletID1s();
    	String[] trackletID2s=fd.getTrackletID2s();
    	IntBuffer indexBuf = indexMat.getIntBuffer();
        //欧式距离的平方
        FloatBuffer distsBuf = distsMat.getFloatBuffer();
        
        //保存所有的信息
        List<ReIdAttributesTemp> list=new ArrayList<>();
       
        //打印索引和距离
        for(int i=0;i<arr2Row;i++){
	        for (int j = i*k; j < (i+1)*k; j++) {
//	            System.out.println("被广播出去的index："+i+",总的index:" + indexBuf.get(j)+",距离:" + distsBuf.get(j));
	            ReIdAttributesTemp reIdAttributesTemp=new ReIdAttributesTemp();
	            reIdAttributesTemp.setSim(distsBuf.get(j));
	            reIdAttributesTemp.setFloatArrLineNum1(indexBuf.get(j));
	            reIdAttributesTemp.setFloatArrLineNum2(i);
	            list.add(reIdAttributesTemp);
	        }
        }
        
        
        //打印mat
        FloatBuffer mat1Buf =mat1.getFloatBuffer();
        FloatBuffer mat2Buf =mat2.getFloatBuffer();
        Map<Integer,float[]> map1=new HashMap<>(); 
        Map<Integer,String> map1id=new HashMap<>();
        Map<Integer,String> map2id=new HashMap<>();
        Map<Integer,float[]> map2=new HashMap<>(); 
        
        for (int i = 0; i < arr1Row; i++) {
//        	float[] arr1float=new float[col];
        	List<Float> arr1list=new ArrayList<>();
        	for (int j = i*col; j < (i+1)*col; ++j) {
        		
        		arr1list.add(mat1Buf.get(j));
//        		System.out.println("总的index:" +i+ ",element:" + mat1Buf.get(j));
        	}
        	map1.put(i, ArrayUtils.toPrimitive(arr1list.toArray(new Float[0]), 0.0F));
        	arr1list=null;
        }
        
      //保存总的id
        for (int j = 0; j < trackletID1s.length; j++) {
        	map1id.put(j, trackletID1s[j]);
        }
        //保存广播出去的id
        for (int j = 0; j < trackletID2s.length; j++) {
        	map2id.put(j, trackletID2s[j]);
        }
        for (int i = 0; i < arr2Row; i++) {
        	List<Float> arr2list=new ArrayList<>();
	        for (int j = i*col; j < (i+1)*col; ++j) {
	        	
	        	arr2list.add(mat2Buf.get(j));
//	        	System.out.println("被广播出去的index:" +i+ ",element:" + mat2Buf.get(j));
	        }
	        map2.put(i,  ArrayUtils.toPrimitive(arr2list.toArray(new Float[0]), 0.0F));
	        arr2list=null;
        }
        
		for (int i = 0; i < list.size(); i++) {
			ReIdAttributesTemp reIdAttributesTemp=list.get(i);
			reIdAttributesTemp.setFloatArr1(map1.get(reIdAttributesTemp.getFloatArrLineNum1()));
			reIdAttributesTemp.setFloatArr2(map2.get(reIdAttributesTemp.getFloatArrLineNum2()));
			reIdAttributesTemp.setTrackletID1(map1id.get(reIdAttributesTemp.getFloatArrLineNum1()));
//			reIdAttributesTemp.setTrackletID2(fd.getTrackletID2());
			reIdAttributesTemp.setTrackletID2(map2id.get(reIdAttributesTemp.getFloatArrLineNum2()));
		}
		
//		for (int i = 0; i < list.size(); i++) {
//			System.out.println("list:"+list.get(i).toString(1));
//		}
		System.out.println("java knn list的大小是："+list.size());
//        System.out.println("every knn 结束------------------------");
        return list;
	}
    
    // Release.
    public void release(FeatureData fd){
    	Mat mat1=fd.getMat1();
    	Mat mat2=fd.getMat2();
    	Mat indexMat=fd.getIndexMat();
    	Mat distsMat=fd.getDistsMat();
    	FloatPointer fp1=fd.getFp1();FloatPointer fp2=fd.getFp2();
    	mat1.close();
    	mat2.close();
    	indexMat.close();
    	distsMat.close();
        fp1.close();
        fp2.close();
        mat1.deallocate();
        mat2.deallocate();
        indexMat.deallocate();
        distsMat.deallocate();
        fp1.deallocate();
        fp2.deallocate();
        flannIndex.close();
//        flannIndex.release();
        flannIndex.deallocate();
    }
    /**
     * 
     * LANG
     * @param arr1 总的float数组
     * @param arr1Row 总的float的行数
     * @param arr2 一份float数组
     * @param arr2Row 一份float数组的行数
     * @param col 维数
     * @param k topk
     */
    @SuppressWarnings({ "deprecation" })
	public List<ReIdAttributesTemp> getKnn(float[] arr1,float[] arr2,int col,int k,JavaKnn javaKnn){
    	
    	int arr1len=arr1.length;
    	int arr2len=arr2.length;
//    	System.out.println("arr1len:"+arr1len+",arr2len"+arr2len);
    	int arr1Row=arr1len/col;
    	int arr2Row=arr2len/col;
    	
    	//得到mat
    	Mat mat1 = new Mat(arr1Row, col, CV_32FC1);
        final FloatPointer fp1 = new FloatPointer(mat1.data());
        fp1.put(arr1);
        						//row //col
        Mat mat2 = new Mat(arr2Row, col, CV_32FC1);
        final FloatPointer fp2 = new FloatPointer(mat2.data());
        fp2.put(arr2);
        
        // Knn search.
//        int k = 5;
//        JavaKnn javaKnn = new JavaKnn();
        javaKnn.init(k, FLANN_DIST_L2);
        long startTime = System.currentTimeMillis();
        javaKnn.knnSearch(mat2, mat1);
        long endTime = System.currentTimeMillis();
		System.out.println("Cost time of ervry knn: " + (endTime - startTime) + "ms");
		
        // Get results.
        Mat indexMat = javaKnn.getIndexMat();
        Mat distsMat = javaKnn.getDistMat();
        //galleryArray中的位置
        IntBuffer indexBuf = indexMat.getIntBuffer();
        //欧式距离的平方
        FloatBuffer distsBuf = distsMat.getFloatBuffer();
        
        //保存所有的信息
        List<ReIdAttributesTemp> list=new ArrayList<>();
       
        //打印索引和距离
        for(int i=0;i<arr2Row;i++){
	        for (int j = i*k; j < (i+1)*k; j++) {
//	            System.out.println("被广播出去的index："+i+",总的index:" + indexBuf.get(j)+",距离:" + distsBuf.get(j));
	            ReIdAttributesTemp reIdAttributesTemp=new ReIdAttributesTemp();
	            reIdAttributesTemp.setSim(distsBuf.get(j));
	            reIdAttributesTemp.setFloatArrLineNum1(indexBuf.get(j));
	            reIdAttributesTemp.setFloatArrLineNum2(i);
	            list.add(reIdAttributesTemp);
	        }
        }
        
        
        //打印mat
        FloatBuffer mat1Buf =mat1.getFloatBuffer();
        FloatBuffer mat2Buf =mat2.getFloatBuffer();
        Map<Integer,float[]> map1=new HashMap<>(); 
        Map<Integer,float[]> map2=new HashMap<>(); 
        
        for (int i = 0; i < arr1Row; i++) {
//        	float[] arr1float=new float[col];
        	List<Float> arr1list=new ArrayList<>();
        	for (int j = i*col; j < (i+1)*col; ++j) {
        		
        		arr1list.add(mat1Buf.get(j));
//        		System.out.println("总的index:" +i+ ",element:" + mat1Buf.get(j));
        	}
        	map1.put(i, ArrayUtils.toPrimitive(arr1list.toArray(new Float[0]), 0.0F));
        	arr1list=null;
        }
        for (int i = 0; i < arr2Row; i++) {
        	List<Float> arr2list=new ArrayList<>();
	        for (int j = i*col; j < (i+1)*col; ++j) {
	        	
	        	arr2list.add(mat2Buf.get(j));
//	        	System.out.println("被广播出去的index:" +i+ ",element:" + mat2Buf.get(j));
	        }
	        map2.put(i,  ArrayUtils.toPrimitive(arr2list.toArray(new Float[0]), 0.0F));
	        arr2list=null;
        }
        
		for (int i = 0; i < list.size(); i++) {
			ReIdAttributesTemp reIdAttributesTemp=list.get(i);
			reIdAttributesTemp.setFloatArr1(map1.get(reIdAttributesTemp.getFloatArrLineNum1()));
			reIdAttributesTemp.setFloatArr2(map2.get(reIdAttributesTemp.getFloatArrLineNum2()));
		}
		
//		for (int i = 0; i < list.size(); i++) {
//			System.out.println("list:"+list.get(i).toString(1));
//		}
		
        // Release.
        fp1.close();
        fp2.close();
        fp1.deallocate();
        fp2.deallocate();
        
        System.out.println("every knn 结束------------------------");
        return list;
    }
    
    
	@SuppressWarnings("deprecation")
	public List<ReIdAttributesTemp> getKnn(Tuple2<Tuple2<String[], float[]>, Tuple2<String, float[]>> tuple
			,int col,int k,JavaKnn javaKnn
//			,List<ReIdAttributesTemp> list
			){
    	float[] arr1=tuple._1()._2();
    	float[] arr2=tuple._2()._2();
    	String[] trackletID1s=tuple._1()._1();
    	int arr1len=arr1.length;
    	int arr2len=arr2.length;
//    	System.out.println("arr1len:"+arr1len+",arr2len:"+arr2len);
    	int arr1Row=arr1len/col;
    	int arr2Row=arr2len/col;
//    	System.out.println("arr1Row:"+arr1Row+",arr2Row:"+arr2Row+",trackletID1s的大小是："+trackletID1s.length);
    	
    	//得到mat
    	Mat mat1 = new Mat(arr1Row, col, CV_32FC1);
        FloatPointer fp1 = new FloatPointer(mat1.data());
        fp1.put(arr1);
        						//row //col
        Mat mat2 = new Mat(arr2Row, col, CV_32FC1);
        FloatPointer fp2 = new FloatPointer(mat2.data());
        fp2.put(arr2);
        
        // Knn search.
//        int k = 5;
//        JavaKnn javaKnn = new JavaKnn();
        javaKnn.init(k, FLANN_DIST_L2);
        long startTime = System.currentTimeMillis();
        javaKnn.knnSearch(mat2, mat1);
        long endTime = System.currentTimeMillis();
//		System.out.println("Cost time of ervry knn: " + (endTime - startTime) + "ms");
		
        // Get results.
        Mat indexMat = javaKnn.getIndexMat();
        Mat distsMat = javaKnn.getDistMat();
//        IntPointer indexMatData = new IntPointer(indexMat.data());
//        FloatPointer distsMatData = new FloatPointer(distsMat.data());
        
        //galleryArray中的位置
        IntBuffer indexBuf = indexMat.getIntBuffer();
//        IntBuffer indexBuf = (IntBuffer)indexMat.asBuffer();
        //欧式距离的平方
        FloatBuffer distsBuf = distsMat.getFloatBuffer();
//        FloatBuffer distsBuf = (FloatBuffer)distsMat.asBuffer();
        
        //保存所有的信息
        List<ReIdAttributesTemp> list=new ArrayList<>();
//        List<ReIdAttributesTemp> list= java.util.Collections.synchronizedList(new ArrayList<ReIdAttributesTemp>());
       
        //打印索引和距离
        for(int i=0;i<arr2Row;i++){
	        for (int j = i*k; j < (i+1)*k; j++) {
//	            System.out.println("被广播出去的index："+i+",总的index:" + indexMatData.get(j)+",距离:" + distsMatData.get(j));
	            ReIdAttributesTemp reIdAttributesTemp=new ReIdAttributesTemp();
	            reIdAttributesTemp.setSim(distsBuf.get(j));
//	            reIdAttributesTemp.setSim(distsMatData.get(j));
	            reIdAttributesTemp.setFloatArrLineNum1(indexBuf.get(j));
//	            reIdAttributesTemp.setFloatArrLineNum1(indexMatData.get(j));
	            reIdAttributesTemp.setFloatArrLineNum2(i);
	            list.add(reIdAttributesTemp);
	        }
        }
        
       
        //打印mat,并保存
        FloatBuffer mat1Buf =mat1.getFloatBuffer();
//        FloatBuffer mat1Buf =(FloatBuffer) mat1.asBuffer();
        FloatBuffer mat2Buf =mat2.getFloatBuffer();
//        FloatBuffer mat2Buf =(FloatBuffer)mat2.asBuffer();
        Map<Integer,float[]> map1=new HashMap<>(); 
        Map<Integer,String> map1id=new HashMap<>(); 
        Map<Integer,float[]> map2=new HashMap<>(); 
        
        for (int i = 0; i < arr1Row; i++) {
//        	float[] arr1float=new float[col];
        	List<Float> arr1list=new ArrayList<>();
        	for (int j = i*col; j < (i+1)*col; ++j) {
        		arr1list.add(mat1Buf.get(j));
//        		arr1list.add(fp1.get(j));
//        		System.out.println("总的index:" +i+ ",element:" + fp1.get(j));
        	}
        	map1.put(i, ArrayUtils.toPrimitive(arr1list.toArray(new Float[0]), 0.0F));
        	arr1list=null;
        }
        //保存总的id
        for (int j = 0; j < trackletID1s.length; j++) {
        	map1id.put(j, trackletID1s[j]);
        }
        
        for (int i = 0; i < arr2Row; i++) {
        	List<Float> arr2list=new ArrayList<>();
	        for (int j = i*col; j < (i+1)*col; ++j) {
	        	arr2list.add(mat2Buf.get(j));
//	        	arr2list.add(fp2.get(j));
//	        	System.out.println("被广播出去的index:" +i+ ",element:" + fp2.get(j));
	        }
	        map2.put(i,  ArrayUtils.toPrimitive(arr2list.toArray(new Float[0]), 0.0F));
	        arr2list=null;
        }
        
		for (int i = 0; i < list.size(); i++) {
			ReIdAttributesTemp reIdAttributesTemp=list.get(i);
			reIdAttributesTemp.setFloatArr1(map1.get(reIdAttributesTemp.getFloatArrLineNum1()));
			reIdAttributesTemp.setFloatArr2(map2.get(reIdAttributesTemp.getFloatArrLineNum2()));
//			reIdAttributesTemp.setTrackletID1s(tuple._1()._1());
			reIdAttributesTemp.setTrackletID2(tuple._2()._1());
			reIdAttributesTemp.setTrackletID1(map1id.get(reIdAttributesTemp.getFloatArrLineNum1()));
		}
		
//		for (int i = 0; i < list.size(); i++) {
//			System.out.println("list:"+list.get(i).toString(1));
//		}
		
        // Release.
//		indexMatData.close();
//	    distsMatData.close();
        fp1.close();
        fp2.close();
        fp1.deallocate();
        fp2.deallocate();
//        indexMatData.deallocate();
//        distsMatData.deallocate();
//        System.out.println("every knn 结束---------------------");
        return list;
    }
    
    
    // Main function.
    @SuppressWarnings("deprecation")
	public static void main( String[] args )
    {
//    	Runtime r = Runtime.getRuntime();  
//    	r.gc();  
//    	long startMem = r.freeMemory(); // 开始时的剩余内存  
        // Source data.
//         float[] galleryArray = new float[1000000*128];
        float[] galleryArray = {1.0f, 2.0f, 3.0f, 4.0f, 5.0f 
        						,2.0f, 3.0f, 4.0f, 5.0f, 6.0f, 
        						1.0f, 2.0f, 3.0f, 4.0f, 6.0f,
				        		9.1f, 6.1f, 6.6f, 7.8f, 2.5f, 
				        		1.0f, 2.0f, 3.0f, 4.0f, 6.0f,
				        		1.0f, 2.0f, 3.0f, 4.0f, 6.0f,
				        		2.0f, 3.0f, 4.0f, 5.0f, 6.0f
				        		};
//        float[] probeArray = new float[1000000*128];
//        for (int i = 0; i < galleryArray.length; i++) {
//     	galleryArray[i]=new Random().nextFloat();
//     	probeArray[i]=new Random().nextFloat();
//		}
        float[] probeArray = {1.0f, 2.0f, 3.0f, 4.0f, 5.0f
        						,	1.0f, 2.0f, 3.0f, 4.0f, 6.0f
        						,1.0f, 2.0f, 3.0f, 4.0f, 6.0f,
        						9.1f, 6.1f, 6.6f, 7.8f, 2.5f
        						};
//        long orz = startMem - r.freeMemory();
//        System.out.println(orz);
        List<ReIdAttributesTemp> list= java.util.Collections.synchronizedList(new ArrayList<ReIdAttributesTemp>());
        JavaKnn javaKnn = new JavaKnn();
//        list=javaKnn.getKnn(galleryArray, probeArray, 5, 9, javaKnn,list);
        System.out.println("-------------------main-------------------------------");
        test();
        getMatDataTest();
        /*Thread1 mTh1=new Thread1("A");  
        Thread1 mTh2=new Thread1("B");  
        Thread1 mTh3=new Thread1("C");  
        mTh1.start();  
        mTh2.start(); 
        mTh3.start();*/ 
        
    }
    
    public static void test(){
   	 // Source data.
//     float[] galleryArray = new float[100000];
   	 float[] galleryArray = {1.0f, 2.0f, 3.0f, 4.0f, 5.0f, 
					2.0f, 3.0f, 4.0f, 5.0f, 6.0f, 
					1.0f, 2.0f, 3.0f, 4.0f, 6.0f,
	        		9.1f, 6.1f, 6.6f, 7.8f, 2.5f, 
	        		1.0f, 2.0f, 3.0f, 4.0f, 6.0f,
	        		1.0f, 2.0f, 3.0f, 4.0f, 6.0f,
	        		2.0f, 3.0f, 4.0f, 5.0f, 6.0f};
//    float[] probeArray = new float[100000];
   	 float[] probeArray = {1.0f, 2.0f, 3.0f, 4.0f, 5.0f,	
					1.0f, 2.0f, 3.0f, 4.0f, 6.0f
					,1.0f, 2.0f, 3.0f, 4.0f, 6.0f,
					9.1f, 6.1f, 6.6f, 7.8f, 2.5f};
    
    /*for (int i = 0; i < galleryArray.length; i++) {
    	galleryArray[i]=new Random().nextFloat();
    	probeArray[i]=new Random().nextFloat();
		}*/
   	 
   	// Mat data.
       						//条数   //维度
       Mat galleryMat = new Mat(7, 5, CV_32FC1);
       final FloatPointer galleryMatData = new FloatPointer(galleryMat.data());
       galleryMatData.put(galleryArray);
       						//row //col
       Mat probesMat = new Mat(4, 5, CV_32FC1);
       final FloatPointer probesMatData = new FloatPointer(probesMat.data());
       probesMatData.put(probeArray);
       
       //不能再次put，put进去的是0
//       probesMatData.put(probeArray);
       
       // Knn search.
       int k = 7;
       JavaKnn javaKnn = new JavaKnn();
       javaKnn.init(k, FLANN_DIST_L2);
       long startTime = System.currentTimeMillis();
       javaKnn.knnSearch(probesMat, galleryMat);
       long endTime = System.currentTimeMillis();
		System.out.println("Cost time of knn: " + (endTime - startTime) + "ms");
		
       // Get results.
       Mat indexMat = javaKnn.getIndexMat();
       Mat distsMat = javaKnn.getDistMat();
       //galleryArray中的位置
       IntBuffer indexBuf = indexMat.getIntBuffer();
       //欧式距离的平方
       FloatBuffer distsBuf = distsMat.getFloatBuffer();

       //打印mat
       FloatBuffer probesMatBuf =probesMat.getFloatBuffer();
       FloatBuffer galleryMatBuf =galleryMat.getFloatBuffer();
//       System.out.println( probesMat.createIndexer().array());
      /* float[] out=probesMatDistsBuf.array();
       System.out.println(out);
       for (int i = 0; i < probeArray.length; i++) {
			
       	System.out.println(out[i]);
		}*/
       for (int i = 0; i < probesMatBuf.capacity(); i++) {
				
       		System.out.println("被广播出去的index:" +",element:" + probesMatBuf.get(i));
       	
		}
       
       for (int i = 0; i < galleryMatBuf.capacity(); i++) {
			
      		System.out.println("总的index:" +",element:" + galleryMatBuf.get(i));
      	
		}
//       System.out.println(indexBuf.capacity());
//       System.out.println(distsBuf.capacity());
       for (int i = 0; i < probesMat.rows()*k; i++) {
           System.out.println("index:" + indexBuf.get(i)+",element:" + distsBuf.get(i));
       }
//       for (int i = 0; i < probesMat.rows()*k; i++) {
//       }
       System.out.println("------------------------");
       // Release.
       galleryMatData.close();
       probesMatData.close();
       galleryMatData.deallocate();
       probesMatData.deallocate();
//       System.out.println("=== DONE ===");
   }


public static void getMatDataTest(){
	// Source data.
	 float[] galleryArray = {1.0f, 2.0f, 3.0f, 4.0f, 5.0f, 
				2.0f, 3.0f, 4.0f, 5.0f, 6.0f, 
				1.0f, 2.0f, 3.0f, 4.0f, 6.0f,
        		9.1f, 6.1f, 6.6f, 7.8f, 2.5f, 
        		1.0f, 2.0f, 3.0f, 4.0f, 6.0f,
        		1.0f, 2.0f, 3.0f, 4.0f, 6.0f,
        		2.0f, 3.0f, 4.0f, 5.0f, 6.0f};
	 float[] probeArray = {1.0f, 2.0f, 3.0f, 4.0f, 5.0f,	
				1.0f, 2.0f, 3.0f, 4.0f, 6.0f
				,1.0f, 2.0f, 3.0f, 4.0f, 6.0f,
				9.1f, 6.1f, 6.6f, 7.8f, 2.5f};
 
	// Mat data.
    						//条数   //维度
    Mat galleryMat = new Mat(7, 5, CV_32FC1);
    final FloatPointer galleryMatData = new FloatPointer(galleryMat.data());
    galleryMatData.put(galleryArray);
    						//row //col
    Mat probesMat = new Mat(4, 5, CV_32FC1);
    final FloatPointer probesMatData = new FloatPointer(probesMat.data());
    probesMatData.put(probeArray);
    
    // Knn search.
    int k = 7;
    JavaKnn javaKnn = new JavaKnn();
    javaKnn.init(k, FLANN_DIST_L2);
    long startTime = System.currentTimeMillis();
    javaKnn.knnSearch(probesMat, galleryMat);
    long endTime = System.currentTimeMillis();
	System.out.println("Cost time of knn: " + (endTime - startTime) + "ms");
	
    // Get results.
    Mat indexMat = javaKnn.getIndexMat();
    Mat distsMat = javaKnn.getDistMat();
    
    IntPointer indexMatData = new IntPointer(indexMat.data());
    FloatPointer distsMatData = new FloatPointer(distsMat.data());
    //打印索引和距离
    for(int i=0;i<probesMat.rows();i++){
        for (int j = i*k; j < (i+1)*k; j++) {
            System.out.println("被广播出去的index："+i+",总的index:" + indexMatData.get(j)+",距离:" + distsMatData.get(j));
        }
    }

    //打印mat
  for (int i = 0; i < probesMat.rows(); i++) {
	  for (int j = i*5; j < (i+1)*5; ++j) {
    	System.out.println("被广播出去的index:" +i+ ",element:" + probesMatData.get(j));
		}
	System.out.println();
}

  for (int i = 0; i < galleryMat.rows(); i++) {
	  for (int j = i*5; j < (i+1)*5; ++j) {
		System.out.println("总的index:" +i+ ",element:" + galleryMatData.get(j));
		}
	System.out.println();
}
    
    System.out.println("------------------------");

    // Release.
    indexMatData.close();
    distsMatData.close();
    galleryMatData.close();
    probesMatData.close();
//    galleryMatData.deallocate();
//    probesMatData.deallocate();
}
}
class Thread1 extends Thread {
	private String name;

	public Thread1(String name) {
		this.name = name;
	}

	public void run() {
		for (int i = 0; i < 5; i++) {
			try {
				float[] galleryArray = { 1.0f, 2.0f, 3.0f, 4.0f, 5.0f, 2.0f, 3.0f, 4.0f, 5.0f, 6.0f, 1.0f, 2.0f, 3.0f,
						4.0f, 6.0f, 9.1f, 6.1f, 6.6f, 7.8f, 2.5f, 1.0f, 2.0f, 3.0f, 4.0f, 6.0f, 1.0f, 2.0f, 3.0f, 4.0f,
						6.0f, 2.0f, 3.0f, 4.0f, 5.0f, 6.0f };
				float[] probeArray = { 1.0f, 2.0f, 3.0f, 4.0f, 5.0f, 1.0f, 2.0f, 3.0f, 4.0f, 6.0f, 1.0f, 2.0f, 3.0f,
						4.0f, 6.0f, 9.1f, 6.1f, 6.6f, 7.8f, 2.5f };
				List<ReIdAttributesTemp> list= java.util.Collections.synchronizedList(new ArrayList<ReIdAttributesTemp>());
		        JavaKnn javaKnn = new JavaKnn();
//		        list=javaKnn.getKnn(galleryArray, probeArray, 5, 7, javaKnn,list);
				System.out.println("-------------------"+name+"-------------------------------");
				System.out.println("thread:"+list);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

	}
}