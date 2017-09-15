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


public class ReIdAttributesTempRDD implements Serializable {

	private int x;
	private int y;
	private String id1;
	private  String id2;
	private ReIdAttributesTemp reIdAttributesTemp1;
	private ReIdAttributesTemp reIdAttributesTemp2;
	public ReIdAttributesTempRDD(int x, int y, String id1, String id2, ReIdAttributesTemp reIdAttributesTemp1,
			ReIdAttributesTemp reIdAttributesTemp2) {
		super();
		this.x = x;
		this.y = y;
		this.id1 = id1;
		this.id2 = id2;
		this.reIdAttributesTemp1 = reIdAttributesTemp1;
		this.reIdAttributesTemp2 = reIdAttributesTemp2;
	}
	public int getX() {
		return x;
	}
	public void setX(int x) {
		this.x = x;
	}
	public int getY() {
		return y;
	}
	public void setY(int y) {
		this.y = y;
	}
	public String getId1() {
		return id1;
	}
	public void setId1(String id1) {
		this.id1 = id1;
	}
	public String getId2() {
		return id2;
	}
	public void setId2(String id2) {
		this.id2 = id2;
	}
	public ReIdAttributesTemp getReIdAttributesTemp1() {
		return reIdAttributesTemp1;
	}
	public void setReIdAttributesTemp1(ReIdAttributesTemp reIdAttributesTemp1) {
		this.reIdAttributesTemp1 = reIdAttributesTemp1;
	}
	public ReIdAttributesTemp getReIdAttributesTemp2() {
		return reIdAttributesTemp2;
	}
	public void setReIdAttributesTemp2(ReIdAttributesTemp reIdAttributesTemp2) {
		this.reIdAttributesTemp2 = reIdAttributesTemp2;
	}
    
}
