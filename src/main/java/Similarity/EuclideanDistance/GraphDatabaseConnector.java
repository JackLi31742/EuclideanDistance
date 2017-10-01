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

package Similarity.EuclideanDistance;

import java.io.Serializable;
import java.util.List;
import java.util.NoSuchElementException;

import javax.annotation.Nonnull;

import Similarity.EuclideanDistance.util.Logger;
import entities.Attributes;
import entities.Feature;
import entities.Hour;
import entities.Minute;
import entities.ReIdAttributesTemp;
import entities.Tracklet;

/**
 * The class GraphDatabaseConnector is the base class of graph databases connectors.
 * Each node in the graph database is a pedestrian.
 *
 * @author Ken Yu, CRIPAC, 2016
 */
public abstract class GraphDatabaseConnector implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = -4442820846952103102L;

	public abstract void release();
    /**
     * Set the path of the directory saving the tracklet of a pedestrian.
     *
     * @param nodeID the id of the pedestrian.
     * @param path   the path of the directory saving the tracklet of the pedestrian.
     */
    public abstract void setTrackletSavingPath(@Nonnull String nodeID,
                                               @Nonnull String path,Logger logger);

    public abstract void setTrackletSavingPath(@Nonnull String nodeID,
            @Nonnull String path);
    public abstract void setTrackletSavingPathFlag(@Nonnull String nodeID,
            @Nonnull Boolean flag,Logger logger);
    
    public abstract void setTrackletSavingVideoPath(@Nonnull String nodeID,@Nonnull String videoPath);
    
    public abstract void saveTrackletImg(@Nonnull String nodeID,@Nonnull int[] width);
    
    public abstract void setSaveTracklet(@Nonnull Tracklet tracklet,Logger logger);
    /**
     * Get the path of the directory saving the tracklet of a pedestrian.
     *
     * @param nodeID the id of the pedestrian.
     * @return the path of the directory saving the tracklet of the pedestrian.
     * @throws NoSuchElementException On failure finding the pedestrian.
     */
    public abstract String getTrackletSavingDir(@Nonnull String nodeID) throws NoSuchElementException;

    /**
     * Set the similarity between two pedestrians.
     *
     * @param idA        the ID of the first pedestrian.
     * @param idB        the ID of the second pedestrian.
     * @param similarity the similarity between them.
     */
    public abstract void setPedestrianSimilarity(@Nonnull String idA,
                                                 @Nonnull String idB,
                                                 float similarity);

    /**
     * Get the similarity between two pedestrians.
     *
     * @param idA the ID of the first pedestrian.
     * @param idB the ID of the second pedestrian.
     * @return The similarity between them.
     * @throws NoSuchElementException On failure finding any of these two pedestrian, or when there
     *                                is no link between them.
     */
    public abstract float getPedestrianSimilarity(@Nonnull String idA,
                                                  @Nonnull String idB) throws NoSuchElementException;

    /**
     * Set the attributes of a pedestrian.
     *
     * @param nodeID the ID of the pedestrian.
     * @param attr   the attributes of the pedestrian.
     */
    public abstract void setPedestrianAttributes(@Nonnull String nodeID,
                                                 @Nonnull Attributes attr
                                                 ,Logger logger);

    /**
     * Get the attributes of a pedestrian.
     *
     * @param nodeID the ID of the pedestrian.
     * @return the attributes of the pedestrian.
     * @throws NoSuchElementException On failure finding the pedestrian.
     */
    public abstract Attributes getPedestrianAttributes(@Nonnull String nodeID) throws NoSuchElementException;

    /**
     * Get relations: (nodA)-[SIMILARITY]-(nodeB)
     *
     * @param nodeID the ID of the pedestrian.
     * @return the relationships related to the input pedestrian.
     * @throws NoSuchElementException On failure finding the pedestrian.
     */
    public abstract Link[] getLinkedPedestrians(@Nonnull String nodeID) throws NoSuchElementException;

    /**
     * The class Link represents a link from one node to another in the graph
     * database.
     *
     * @author Ken Yu, CRIPAC, 2016
     */
    public static class Link {
        public String nodeA;
        public String nodeB;
        public float similarity;

        public Link() {
        }

        public Link(@Nonnull String nodeA,
                    @Nonnull String nodeB,
                    float similarity) {
            this.nodeA = nodeA;
            this.nodeB = nodeB;
            this.similarity = similarity;
        }
    }
    
    
    /**
     * Set the reid feature of a pedestrian.
     * 
     * @param nodeID   the ID of the pedestrian.
     * @param dataType it depends on the execute plan (the algorithm to generate such result).
     * @param fea      the reid feature of the pedestrian.
     */
    public abstract void setPedestrianReIDFeature(@Nonnull String nodeID,
                                                  @Nonnull String dataType,
                                                  @Nonnull Feature fea);
    
    
    /**
     * Get the reid feature of a pedestrian.
     *
     * @param nodeID the ID of the pedestrian.
     * @param dataType user plan.
     * @return the reid feature of the pedestrian.
     * @throws NoSuchElementException On failure finding the pedestrian.
     */
    public abstract Feature getPedestrianReIDFeature(@Nonnull String nodeID,
                                                     @Nonnull String dataType) throws NoSuchElementException;
    
    //得到符合条件的FeatureList,目前使用dataType=“dli_test”做判断
    public abstract List<ReIdAttributesTemp> getPedestrianReIDFeatureList(@Nonnull String dataType)throws NoSuchElementException;
    
    //得到符合条件的FeatureList,isFinish=true，IsGetSim=false
    public abstract List<float[]> getPedestrianReIDFeatureList(@Nonnull boolean isFinish,boolean IsGetSim)throws NoSuchElementException;
    
    //得到Minute节点
    public abstract List<Minute> getMinutes();
    //得到Hour节点
    public abstract List<Hour> getHours();
//    //得到day
//    public abstract List<Day> getDays(Month month);
//    //得到month
//    public abstract List<Month> getMonths(Year year);
//    //得到year
//    public abstract List<Year> getYears();
    
    //根据minute，得到符合条件的节点，用来做相似度计算
    public abstract List<ReIdAttributesTemp> getPedestrianReIDFeatureList(Minute minute)throws NoSuchElementException;
    
    //通过Hour得到符合条件的Person节点
    public abstract List<ReIdAttributesTemp> getPedestrianReIDFeatureList(Hour hour) throws NoSuchElementException;
    
    
    
    //得到符合条件的FeatureList中的base64，用作测试
    public abstract List<String> getPedestrianReIDFeatureBase64List(@Nonnull String dataType) throws NoSuchElementException;
    
    //标志位，是否完成了reid
    public abstract void addIsFinish(@Nonnull String nodeID,@Nonnull boolean isFinish);
    
    //标志位，是否完成了计算相似度
    //做完相似度计算后，设置IsGetSim=true
    public abstract void addIsGetSim(@Nonnull String nodeID,@Nonnull boolean IsGetSim);
    
    
    //增加一条边,保存相似度,minute
    public abstract List<ReIdAttributesTemp> addSimRel(@Nonnull String nodeID1,@Nonnull String nodeID2,@Nonnull double SimRel);
    
  //增加一条边,保存相似度,hour
    public abstract List<ReIdAttributesTemp> addHourSimRel(@Nonnull String nodeID1,@Nonnull String nodeID2,@Nonnull double SimRel);
    
    //判断是否已经存在这条边,如果有，则为true
    public abstract boolean isaddSimRel(@Nonnull String nodeID1,@Nonnull String nodeID2,@Nonnull double SimRel);
    
    //对某一节点增加n条边,保存相似度
    
//    public abstract void addOneToNSimRel(@Nonnull String nodeID1,@Nonnull String nodeID2,@Nonnull double SimRel);
    
    
    //删除节点
    public abstract void delNode( String nodeID1) ;
    //判断是否session还在开着
    public abstract boolean isOpen();
    
    //通过节点id得到相似度
    public abstract List<ReIdAttributesTemp> getPersonSimList(String nodeID1) throws NoSuchElementException;
    
    public abstract void copyNodes() ;
    
    public abstract void restore();
    
    public abstract List<ReIdAttributesTemp> getPedestrianReIDFeatureList();
}
