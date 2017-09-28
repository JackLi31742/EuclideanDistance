/*
 * This file is part of LaS-VPE Platform.
 *
 * LaS-VPE Platform is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * LaS-VPE Platform is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with LaS-VPE Platform.  If not, see <http://www.gnu.org/licenses/>.
 */

package entities;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map.Entry;

import org.apache.commons.collections4.keyvalue.MultiKey;
import org.apache.commons.collections4.map.MultiKeyMap;

import com.google.gson.Gson;


public class ReIdAttributesTemp implements Serializable {


    
	private static final long serialVersionUID = -3229599105288776061L;
	
	/**
	 * 节点信息
	 */
	private String camID;
    private String trackletID;
    private Long startTime;
    private float[] featureVector;
    private String dataType;
    
    //父节点minute的start
    private Long start;
    //节点之间的相似度
    private double sim;
    
    //节点的id
    private String trackletID1;
    private String trackletID2;
    //节点的float数组
    private float[] floatArr1;
    private float[] floatArr2;
    /**
     * 每个partition中总的行号
     */
    private int floatArrLineNum1;
    /**
     * 每个partition中被广播出去的行号
     */
    private int floatArrLineNum2;
    
    /**
     * 每个partition中总的ids
     */
    private String[] trackletID1s;
    
    //本来用来保存节点以及相似度的，用了spark之后就不用了
    MultiKeyMap<String,Double>  euclideanDistanceSimilarities;

	
   

	public String getDataType() {
		return dataType;
	}

	public void setDataType(String dataType) {
		this.dataType = dataType;
	}

	public String[] getTrackletID1s() {
		return trackletID1s;
	}

	public void setTrackletID1s(String[] trackletID1s) {
		this.trackletID1s = trackletID1s;
	}

	public int getFloatArrLineNum1() {
		return floatArrLineNum1;
	}

	public void setFloatArrLineNum1(int floatArrLineNum1) {
		this.floatArrLineNum1 = floatArrLineNum1;
	}

	public int getFloatArrLineNum2() {
		return floatArrLineNum2;
	}

	public void setFloatArrLineNum2(int floatArrLineNum2) {
		this.floatArrLineNum2 = floatArrLineNum2;
	}

	public float[] getFloatArr1() {
		return floatArr1;
	}

	public void setFloatArr1(float[] floatArr1) {
		this.floatArr1 = floatArr1;
	}

	public float[] getFloatArr2() {
		return floatArr2;
	}

	public void setFloatArr2(float[] floatArr2) {
		this.floatArr2 = floatArr2;
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

	public double getSim() {
		return sim;
	}

	public void setSim(double sim) {
		this.sim = sim;
	}

	public Long getStart() {
		return start;
	}

	public void setStart(Long start) {
		this.start = start;
	}
	public String getTrackletID() {
		return trackletID;
	}

	public void setTrackletID(String trackletID) {
		this.trackletID = trackletID;
	}

	public String getCamID() {
		return camID;
	}

	public void setCamID(String camID) {
		this.camID = camID;
	}

	


	public float[] getFeatureVector() {
		return featureVector;
	}

	public void setFeatureVector(float[] featureVector) {
		this.featureVector = featureVector;
	}

	public Long getStartTime() {
		return startTime;
	}

	public void setStartTime(Long startTime) {
		this.startTime = startTime;
	}
	
	
	
	public MultiKeyMap<String, Double> getEuclideanDistanceSimilarities() {
		return euclideanDistanceSimilarities;
	}

	public void setEuclideanDistanceSimilarities(MultiKeyMap<String, Double> euclideanDistanceSimilarities) {
		this.euclideanDistanceSimilarities = euclideanDistanceSimilarities;
	}

	@Override
	public String toString() {
		return new Gson().toJson(this);
	}
	
	
	
	public static void main(String[] args) {
		ReIdAttributesTemp reIdAttributesTemp=new ReIdAttributesTemp();
		MultiKeyMap<String, Double> euclideanDistanceSimilarities =new MultiKeyMap<>();
		euclideanDistanceSimilarities.put("a","b", 30.0);
		reIdAttributesTemp.setEuclideanDistanceSimilarities(euclideanDistanceSimilarities);
		System.out.println(reIdAttributesTemp.getEuclideanDistanceSimilarities());
		
			for (Entry<MultiKey<? extends String>, Double> entryOut : euclideanDistanceSimilarities.entrySet()) {
				MultiKey<? extends String> sim1Keys= entryOut.getKey();
				Double sim1=entryOut.getValue();
//				System.out.println(sim1Keys.getKeys());
//				System.out.println(sim1);\
				String s=sim1Keys.getKey(1);
				System.out.println(s);;
//				String key1=new String(sim1Keys.getKeys()[0]);
//				String key2=new String(sim1Keys.getKeys()[1]);
//				System.out.println(key1+" "+key2+" "+sim1);
		}
		double a=0.0,b=0.0;
		System.out.println(a+" "+b);
	}

	public String toString(int a) {
		return "ReIdAttributesTemp [floatArr1="+floatArr1[0]+",floatArr1的大小是：" + floatArr1.length+ ", floatArrLineNum1=" + floatArrLineNum1+ ", floatArr2="
				+ floatArr2[0]+",floatArr2的大小是："+floatArr2.length  + ", floatArrLineNum2="
				+ floatArrLineNum2+", trackletID1="
						+ trackletID1 +", trackletID2=" + trackletID2 +",sim=" + sim   + "]";
	}

	/*@Override
	public String toString() {
		return "ReIdAttributesTemp [camID=" + camID + ", trackletID=" + trackletID + ", startTime=" + startTime
				+ ", featureVector=" + Arrays.toString(featureVector) + ", start=" + start + ", sim=" + sim
				+ ", trackletID1=" + trackletID1 + ", trackletID2=" + trackletID2 + ", floatArr1="
				+ Arrays.toString(floatArr1) + ", floatArr2=" + Arrays.toString(floatArr2) + ", floatArrLineNum1="
				+ floatArrLineNum1 + ", floatArrLineNum2=" + floatArrLineNum2 + ", trackletID1s="
				+ Arrays.toString(trackletID1s) + ", euclideanDistanceSimilarities=" + euclideanDistanceSimilarities
				+ "]";
	}
*/
	
	
    
}
