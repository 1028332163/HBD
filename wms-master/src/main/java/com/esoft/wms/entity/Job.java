package com.esoft.wms.entity;

public class Job {
	
	private Integer id;
	private String jobUuid;	
	private String serialId;
	private String itemUuid;
	
	private String sonItemUuid;
	private String parentNum;
	private String endParentNum = "0";//自己的父节点有几个已经执行完毕，根据此字段决定是否执行自己
	
	private String taskInsJson;	
	
	public String getSerialId() {
		return serialId;
	}

	public void setSerialId(String serialId) {
		this.serialId = serialId;
	}
	
	public String getSonItemUuid() {
		return sonItemUuid;
	}

	public void setSonItemUuid(String sonItemUuid) {
		this.sonItemUuid = sonItemUuid;
	}
	
	public String getParentNum() {
		return parentNum;
	}

	public void setParentNum(String parentNum) {
		this.parentNum = parentNum;
	}
	
	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public String getEndParentNum() {
		return endParentNum;
	}

	public void setEndParentNum(String endParentNum) {
		this.endParentNum = endParentNum;
	}

	public String getJobUuid() {
		return jobUuid;
	}

	public void setJobUuid(String jobUuid) {
		this.jobUuid = jobUuid;
	}

	public String getTaskInsJson() {
		return taskInsJson;
	}

	public void setTaskInsJson(String taskInsJson) {
		this.taskInsJson = taskInsJson;
	}

	public String getItemUuid() {
		return itemUuid;
	}

	public void setItemUuid(String itemUuid) {
		this.itemUuid = itemUuid;
	}
    public boolean equals(Job job){
    	return this.jobUuid.equals(job.getJobUuid());
    }
}
