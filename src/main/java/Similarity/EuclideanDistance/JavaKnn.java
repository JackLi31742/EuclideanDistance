package Similarity.EuclideanDistance;

import static org.bytedeco.javacpp.opencv_core.CV_32FC1;
import static org.bytedeco.javacpp.opencv_core.CV_32SC1;
import static org.bytedeco.javacpp.opencv_flann.EUCLIDEAN;
import static org.bytedeco.javacpp.opencv_flann.FLANN_DIST_EUCLIDEAN;
import static org.bytedeco.javacpp.opencv_flann.FLANN_DIST_HAMMING;
import static org.bytedeco.javacpp.opencv_flann.FLANN_DIST_L2;

import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.util.Arrays;
import java.util.Random;

import org.bytedeco.javacpp.FloatPointer;
import org.bytedeco.javacpp.opencv_core.Mat;
import org.bytedeco.javacpp.opencv_flann.Index;
import org.bytedeco.javacpp.opencv_flann.IndexParams;
import org.bytedeco.javacpp.opencv_flann.KDTreeIndexParams;
import org.bytedeco.javacpp.opencv_flann.LshIndexParams;
import org.bytedeco.javacpp.opencv_flann.SearchParams;

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
    private SearchParams searchParams = null;
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
        searchParams = new SearchParams(64, 0, true); // maximum number of leafs checked.
    }

    // Knn search.
    public void knnSearch(Mat probes, Mat gallery) {
        int rows = probes.rows();
        indexMat = new Mat(rows, k, CV_32SC1);
        distMat = new Mat(rows, k, CV_32FC1);
        // find nearest neighbors using FLANN
        // TODO: If it can be built only once?
        flannIndex.build(gallery, indexParams, method);
        flannIndex.knnSearch(probes, indexMat, distMat, k, searchParams);
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
    @SuppressWarnings({ "unused", "deprecation" })
	public void getKnn(float[] arr1,float[] arr2,int col,int k,JavaKnn javaKnn){
    	int arr1Row=arr1.length/col;
    	int arr2Row=arr2.length/col;
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
		System.out.println("Cost time of knn: " + (endTime - startTime) + "ms");
		
        // Get results.
       /* Mat indexMat = javaKnn.getIndexMat();
        Mat distsMat = javaKnn.getDistMat();
        //galleryArray中的位置
        IntBuffer indexBuf = indexMat.getIntBuffer();
        //欧式距离的平方
        FloatBuffer distsBuf = distsMat.getFloatBuffer();

        
        //打印mat
        FloatBuffer probesMatDistsBuf =mat2.getFloatBuffer();
        for (int i = 0; i < probesMatDistsBuf.capacity(); i++) {
				
        		System.out.println("index:" +",element:" + probesMatDistsBuf.get(i));
        	
		}
        System.out.println("------------------------");
        //打印索引和距离
        for (int i = 0; i < mat2.rows()*k; i++) {
            System.out.println("index:" + indexBuf.get(i)+",element:" + distsBuf.get(i));
        }
        */
        // Release.
        fp1.close();
        fp2.close();
        fp1.deallocate();
        fp2.deallocate();
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
        float[] galleryArray = {1.0f, 2.0f, 3.0f, 4.0f, 5.0f, 
        						2.0f, 3.0f, 4.0f, 5.0f, 6.0f, 
        						1.0f, 2.0f, 3.0f, 4.0f, 6.0f,
				        		9.1f, 6.1f, 6.6f, 7.8f, 2.5f, 
				        		1.0f, 2.0f, 3.0f, 4.0f, 6.0f};
//        float[] probeArray = new float[1000000*128];
//        for (int i = 0; i < galleryArray.length; i++) {
//     	galleryArray[i]=new Random().nextFloat();
//     	probeArray[i]=new Random().nextFloat();
//		}
        float[] probeArray = {1.0f, 2.0f, 3.0f, 4.0f, 5.0f,	
        						1.0f, 2.0f, 3.0f, 4.0f, 6.0f};
//        long orz = startMem - r.freeMemory();
//        System.out.println(orz);
        JavaKnn javaKnn = new JavaKnn();
//        javaKnn.getKnn(galleryArray, 5, probeArray, 2, 5, 5, javaKnn);
        
        
    }
    
    public void test(){
    	 // Source data.
//      float[] galleryArray = new float[100000];
     float[] galleryArray = {1.0f, 2.0f, 3.0f, 4.0f, 5.0f, 
     						2.0f, 3.0f, 4.0f, 5.0f, 6.0f, 
     						1.0f, 2.0f, 3.0f, 4.0f, 6.0f,
				        		9.1f, 6.1f, 6.6f, 7.8f, 2.5f, 
				        		1.0f, 2.0f, 3.0f, 4.0f, 6.0f};
//     float[] probeArray = new float[100000];
     float[] probeArray = {1.0f, 2.0f, 3.0f, 4.0f, 5.0f,	
     						1.0f, 2.0f, 3.0f, 4.0f, 6.0f};
     
     /*for (int i = 0; i < galleryArray.length; i++) {
     	galleryArray[i]=new Random().nextFloat();
     	probeArray[i]=new Random().nextFloat();
		}*/
    	// Mat data.
        						//条数   //维度
        Mat galleryMat = new Mat(5, 5, CV_32FC1);
        final FloatPointer galleryMatData = new FloatPointer(galleryMat.data());
        galleryMatData.put(galleryArray);
        						//row //col
        Mat probesMat = new Mat(2, 5, CV_32FC1);
        final FloatPointer probesMatData = new FloatPointer(probesMat.data());
        probesMatData.put(probeArray);
        //不能再次put，put进去的是0
//        probesMatData.put(probeArray);
        
        // Knn search.
        int k = 5;
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
        FloatBuffer probesMatDistsBuf =probesMat.getFloatBuffer();
//        System.out.println( probesMat.createIndexer().array());
       /* float[] out=probesMatDistsBuf.array();
        System.out.println(out);
        for (int i = 0; i < probeArray.length; i++) {
			
        	System.out.println(out[i]);
		}*/
        for (int i = 0; i < probesMatDistsBuf.capacity(); i++) {
				
        		System.out.println("index:" +",element:" + probesMatDistsBuf.get(i));
        	
		}
        System.out.println("------------------------");
//        System.out.println(indexBuf.capacity());
//        System.out.println(distsBuf.capacity());
        for (int i = 0; i < probesMat.rows()*k; i++) {
            System.out.println("index:" + indexBuf.get(i)+",element:" + distsBuf.get(i));
        }
//        for (int i = 0; i < probesMat.rows()*k; i++) {
//        }
        // Release.
        galleryMatData.close();
        probesMatData.close();
        galleryMatData.deallocate();
        probesMatData.deallocate();
//        System.out.println("=== DONE ===");
    }
}
