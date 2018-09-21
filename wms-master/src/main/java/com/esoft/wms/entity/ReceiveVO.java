package com.esoft.wms.entity;

import java.util.List;

public class ReceiveVO {
	private List<Job> exeJobs;

	private List<Job> waitJobs;

	public List<Job> getExeJobs() {
		return exeJobs;
	}

	public void setExeJobs(List<Job> exeJobs) {
		this.exeJobs = exeJobs;
	}

	public List<Job> getWaitJobs() {
		return waitJobs;
	}

	public void setWaitJobs(List<Job> waitJobs) {
		this.waitJobs = waitJobs;
	}
	


	
}
