package com.esoft.dp.entity;

import java.io.Serializable;


/**
 * 记录正在执行的任务
 * 
 * @author taosh
 *
 */
public class TaskInfo extends Ancestor implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -2875912996835503724L;

	private Integer id;
	
	private String name;
	
	private String tarskTID;
	
	private String tarskTname;
	
	private String taskClass;
	
	private String taskPackage;	
	
	private String driverMem;
	
	private String executorMem;
	
	private String bashPath;
	
	private String numExecutors;
	
	
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

	public String getBashPath() {
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

}
