package com.esoft.wms.entity;

public class ReturnItem {
	private String taskId;
	private String itemUuid;
	private String status;
	public String getTaskId() {
		return taskId;
	}
	public void setTaskId(String taskId) {
		this.taskId = taskId;
	}
	public String getStatus() {
		return status;
	}
	public void setStatus(String status) {
		this.status = status;
	}
	public String getItemUuid() {
		return itemUuid;
	}
	public void setItemUuid(String itemUuid) {
		this.itemUuid = itemUuid;
	}

	
}
