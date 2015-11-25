package com.fwhale.model;

import java.io.Serializable;
import java.sql.Timestamp;

/**
 * 
 * @author liushubei
 * @date 2014-5-30
 */
public class Cate implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3102828084322579783L;
	private int id;
	private String name;
	private Timestamp updateTime;
	private String des;
	private int userId;
	
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public Timestamp getUpdateTime() {
		return updateTime;
	}
	public void setUpdateTime(Timestamp updateTime) {
		this.updateTime = updateTime;
	}
	public String getDes() {
		return des;
	}
	public void setDes(String des) {
		this.des = des;
	}
	
	
	public int getUserId() {
		return userId;
	}
	public void setUserId(int userId) {
		this.userId = userId;
	}
	@Override
	public String toString() {
		return "Cate [id=" + id + ", name=" + name + ", updateTime="
				+ updateTime + ", des=" + des + ", userId=" + userId + "]";
	}
	
}