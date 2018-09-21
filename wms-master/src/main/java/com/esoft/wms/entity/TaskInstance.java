package com.esoft.wms.entity;

public class TaskInstance {
	
	private String alArg;
	private String driverMem;
	private String executorMem;	
	private Integer id;
	private String insStatus = "start";
	private String name;
	private String numExecutors;
	private String projectId;
	private String sparkCmdContent;
	private String taskBeginTime;
	private String taskId;
	private String jobUuid;
	private String itemUuid;
	private String mechanismId;

	public String getMechanismId() {
		return mechanismId;
	}

	public void setMechanismId(String mechanismId) {
		this.mechanismId = mechanismId;
	}

	public String getItemUuid() {
		return itemUuid;
	}

	public void setItemUuid(String itemUuid) {
		this.itemUuid = itemUuid;
	}	
	public String getJobUuid() {
		return jobUuid;
	}
	public void setJobUuid(String jobUuid) {
		this.jobUuid = jobUuid;
	}
	public String getInsStatus() {
		return insStatus;
	}
	public void setInsStatus(String insStatus) {
		this.insStatus = insStatus;
	}
	public String getDriverMem() {
		return driverMem;
	}
	public void setDriverMem(String driverMem) {
		this.driverMem = driverMem;
	}
	public String getExecutorMem() {
		return executorMem;
	}
	public void setExecutorMem(String executorMem) {
		this.executorMem = executorMem;
	}
	public String getNumExecutors() {
		return numExecutors;
	}
	public void setNumExecutors(String numExecutors) {
		this.numExecutors = numExecutors;
	}
	public String getSparkCmdContent() {
		return sparkCmdContent;
	}
	public void setSparkCmdContent(String sparkCmdContent) {
		this.sparkCmdContent = sparkCmdContent;
	}
	public Integer getId() {
		return id;
	}
	public void setId(Integer id) {
		this.id = id;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getProjectId() {
		return projectId;
	}
	public void setProjectId(String projectId) {
		this.projectId = projectId;
	}
	public String getTaskId() {
		return taskId;
	}
	public void setTaskId(String taskId) {
		this.taskId = taskId;
	}
	public String getTaskBeginTime() {
		return taskBeginTime;
	}
	public void setTaskBeginTime(String taskBeginTime) {
		this.taskBeginTime = taskBeginTime;
	}

	public String getAlArg() {
		return alArg;
	}
	public void setAlArg(String alArg) {
		this.alArg = alArg.replace("`", "");
	}
}
