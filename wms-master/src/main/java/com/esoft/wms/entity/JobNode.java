package com.esoft.wms.entity;

public class JobNode {
	private String itemUuid;
	private String sonItemUuid;
	private String jobUuid;
	private boolean hasScan;
	public String getJobUuid() {
		return jobUuid;
	}
	public void setJobUuid(String jobUuid) {
		this.jobUuid = jobUuid;
	}
	public String getItemUuid() {
		return itemUuid;
	}
	public void setItemUuid(String itemUuid) {
		this.itemUuid = itemUuid;
	}
	public String getSonItemUuid() {
		return sonItemUuid;
	}
	public void setSonItemUuid(String sonItemUuid) {
		this.sonItemUuid = sonItemUuid;
	}
	public boolean isHasScan() {
		return hasScan;
	}
	public void setHasScan(boolean hasScan) {
		this.hasScan = hasScan;
	}
}
