package com.esoft.wms.entity;

public class EndJob {
	private String jobUuid;
	private String itemUuid;
	private String serialId;
	private ReturnItem returnVO;

	public String getItemUuid() {
		return itemUuid;
	}
	public void setItemUuid(String itemUuid) {
		this.itemUuid = itemUuid;
	}
	public String getSerialId() {
		return serialId;
	}
	public void setSerialId(String serialId) {
		this.serialId = serialId;
	}
	public String getJobUuid() {
		return jobUuid;
	}
	public void setJobUuid(String jobUuid) {
		this.jobUuid = jobUuid;
	}
	public ReturnItem getReturnVO() {
		return returnVO;
	}
	public void setReturnVO(ReturnItem returnVO) {
		this.returnVO = returnVO;
	}
	
}
