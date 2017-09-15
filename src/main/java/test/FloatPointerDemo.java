package test;

import static org.bytedeco.javacpp.opencv_core.CV_32FC1;
import static org.bytedeco.javacpp.opencv_core.CV_32FC2;
import static org.bytedeco.javacpp.opencv_core.CV_32SC1;
import static org.bytedeco.javacpp.opencv_core.CV_64FC1;
import static org.bytedeco.javacpp.opencv_core.CV_8UC1;
import static org.bytedeco.javacpp.opencv_core.cvarrToMat;
import static org.bytedeco.javacpp.opencv_flann.FLANN_DIST_HAMMING;

import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.logging.Logger;

import org.bytedeco.javacpp.FloatPointer;
import org.bytedeco.javacpp.Loader;
import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacpp.opencv_core.IplImage;
import org.bytedeco.javacpp.opencv_core.KeyPointVector;
import org.bytedeco.javacpp.opencv_core.Mat;
import org.bytedeco.javacpp.opencv_features2d.AKAZE;
import org.bytedeco.javacpp.opencv_flann.Index;
import org.bytedeco.javacpp.opencv_flann.IndexParams;
import org.bytedeco.javacpp.opencv_flann.LshIndexParams;
import org.bytedeco.javacpp.opencv_flann.SearchParams;
import org.bytedeco.javacv.BaseChildSettings;
import org.bytedeco.javacv.ObjectFinder;
public class FloatPointerDemo {
	public static class Settings extends BaseChildSettings {
        IplImage objectImage = null;
        AKAZE detector = AKAZE.create();
        double distanceThreshold = 0.75;
        int matchesMin = 4;
        double ransacReprojThreshold = 1.0;
        boolean useFLANN = false;

        public IplImage getObjectImage() {
            return objectImage;
        }
        public void setObjectImage(IplImage objectImage) {
            this.objectImage = objectImage;
        }

        public int getDescriptorType() {
            return detector.getDescriptorType();
        }
        public void setDescriptorType(int dtype) {
            detector.setDescriptorType(dtype);
        }

        public int getDescriptorSize() {
            return detector.getDescriptorSize();
        }
        public void setDescriptorSize(int dsize) {
            detector.setDescriptorSize(dsize);
        }

        public int getDescriptorChannels() {
            return detector.getDescriptorChannels();
        }
        public void setDescriptorChannels(int dch) {
            detector.setDescriptorChannels(dch);
        }

        public double getThreshold() {
            return detector.getThreshold();
        }
        public void setThreshold(double threshold) {
            detector.setThreshold(threshold);
        }

        public int getNOctaves() {
            return detector.getNOctaves();
        }
        public void setNOctaves(int nOctaves) {
            detector.setNOctaves(nOctaves);
        }

        public int getNOctaveLayers() {
            return detector.getNOctaveLayers();
        }
        public void setNOctaveLayers(int nOctaveLayers) {
            detector.setNOctaveLayers(nOctaveLayers);
        }

        public double getDistanceThreshold() {
            return distanceThreshold;
        }
        public void setDistanceThreshold(double distanceThreshold) {
            this.distanceThreshold = distanceThreshold;
        }

        public int getMatchesMin() {
            return matchesMin;
        }
        public void setMatchesMin(int matchesMin) {
            this.matchesMin = matchesMin;
        }

        public double getRansacReprojThreshold() {
            return ransacReprojThreshold;
        }
        public void setRansacReprojThreshold(double ransacReprojThreshold) {
            this.ransacReprojThreshold = ransacReprojThreshold;
        }

        public boolean isUseFLANN() {
            return useFLANN;
        }
        public void setUseFLANN(boolean useFLANN) {
            this.useFLANN = useFLANN;
        }
    }

    Settings settings;
    public Settings getSettings() {
        return settings;
    }
    public void setSettings(Settings settings) {
        this.settings = settings;

        objectKeypoints = new KeyPointVector();
        objectDescriptors = new Mat();
        settings.detector.detectAndCompute(cvarrToMat(settings.objectImage),
                new Mat(), objectKeypoints, objectDescriptors, false);

        int total = (int)objectKeypoints.size();
        if (settings.useFLANN) {
            indicesMat = new Mat(total, 2, CV_32FC1);
            distsMat   = new Mat(total, 2, CV_32FC1);
            flannIndex = new Index();
            indexParams = new LshIndexParams(12, 20, 2); // using LSH Hamming distance
            searchParams = new SearchParams(64, 0, true); // maximum number of leafs checked
            searchParams.deallocate(false); // for some reason FLANN seems to do it for us
        }
        pt1  = new Mat(total, 1, CV_32FC1);
        pt2  = new Mat(total, 1, CV_32FC1);
        mask = new Mat(total, 1, CV_32FC1);
        H    = new Mat(3, 3, CV_32FC1);
        ptpairs = new ArrayList<Integer>(2*objectDescriptors.rows());
        logger.info(total + " object descriptors");
    }

    static final Logger logger = Logger.getLogger(ObjectFinder.class.getName());

    KeyPointVector objectKeypoints = null, imageKeypoints = null;
    Mat objectDescriptors = null, imageDescriptors = null;
    Mat indicesMat, distsMat;
    Index flannIndex = null;
    IndexParams indexParams = null;
    SearchParams searchParams = null;
    Mat pt1 = null, pt2 = null, mask = null, H = null;
    ArrayList<Integer> ptpairs = null;
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Loader.load(opencv_core.class);
		float[] arr1={1.0f,2.0f,3.0f,5.0f};
		opencv_core.Mat mat1 =getMat(arr1);
	}

	
	public static opencv_core.Mat getMat(float[] array) {
        FloatPointer fp=new FloatPointer(array);
        opencv_core.Mat mat = new opencv_core.Mat(fp);
        return mat;
    }
	
	void flannFindPairs(Mat objectDescriptors, Mat imageDescriptors) {
        int length = objectDescriptors.rows();

        // find nearest neighbors using FLANN
        flannIndex.build(imageDescriptors, indexParams, FLANN_DIST_HAMMING);
        flannIndex.knnSearch(objectDescriptors, indicesMat, distsMat, 2, searchParams);

        IntBuffer indicesBuf = indicesMat.createBuffer();
        IntBuffer distsBuf = distsMat.createBuffer();
        for (int i = 0; i < length; i++) {
            if (distsBuf.get(2*i) < settings.distanceThreshold*distsBuf.get(2*i+1)) {
                ptpairs.add(i);
                ptpairs.add(indicesBuf.get(2*i));
            }
        }
    }
}
