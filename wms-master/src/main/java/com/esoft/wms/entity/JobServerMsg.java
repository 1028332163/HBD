package com.esoft.wms.entity;

public class JobServerMsg {
	//job 运行状态
	private String status;
	//成功运行任务的jobId
	private String jobId;

	public String getJobId() {
		return jobId;
	}

	public void setJobId(String jobId) {
		this.jobId = jobId;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}
}
