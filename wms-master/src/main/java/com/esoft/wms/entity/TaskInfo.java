package com.esoft.wms.entity;

import java.io.Serializable;


/**
 * 记录正在执行的任务
 * 
 * @author taosh
 *
 */
public class TaskInfo implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -2875912996835503724L;

	private Integer id;
	
	private String name;
	
	private Integer pID;
	//类别
	private String category;
	
	private String tarskTID;
	
	private String tarskTname;
	
	private String tarskStatus;
	
	private String taskClass;
	
	private String taskPackage;
	
	private String driverMem;
	
	private String driverClass;
	
	private String executorMem;
	
	private String taskParam;
	
	private String bashPath;
	
	private String numExecutors;
	
	//处理参数的类，完全限定名
	private String paramClass;
	
	
	public String getParamClass() {
		return paramClass;
	}

	public void setParamClass(String paramClass) {
		this.paramClass = paramClass;
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

	public String info() {
		return bashPath;
	}

	public void setBashPath(String bashPath) {
		this.bashPath = bashPath;
	}

	public String getNumExecutors() {
		return numExecutors;
	}

	public void setNumExecutors(String numExecutors) {
		this.numExecutors = numExecutors;
	}

	public String getTaskParam() {
		return taskParam;
	}

	public void setTaskParam(String taskParam) {
		this.taskParam = taskParam;
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

	public String getTarskTID() {
		return tarskTID;
	}

	public void setTarskTID(String tarskTID) {
		this.tarskTID = tarskTID;
	}

	public String getTarskTname() {
		return tarskTname;
	}

	public void setTarskTname(String tarskTname) {
		this.tarskTname = tarskTname;
	}

	public String getTarskStatus() {
		return tarskStatus;
	}

	public void setTarskStatus(String tarskStatus) {
		this.tarskStatus = tarskStatus;
	}

	public String getTaskClass() {
		return taskClass;
	}

	public void setTaskClass(String taskClass) {
		this.taskClass = taskClass;
	}

	public String getTaskPackage() {
		return taskPackage;
	}

	public void setTaskPackage(String taskPackage) {
		this.taskPackage = taskPackage;
	}
	public String getBashPath() {
		return bashPath;
	}
	
	public Integer getpID() {
		return pID;
	}

	public void setpID(Integer pID) {
		this.pID = pID;
	}

	public String getCategory() {
		return category;
	}

	public void setCategory(String category) {
		this.category = category;
	}

	public String getDriverClass() {
		return driverClass;
	}

	public void setDriverClass(String driverClass) {
		this.driverClass = driverClass;
	}
	
}
