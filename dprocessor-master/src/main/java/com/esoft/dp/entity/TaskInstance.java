package com.esoft.dp.entity;

public class TaskInstance {
	
	private Integer id;
	
	private String name;
	
	private String insStatus;
	
	private Integer childID;
	
	private String tableName;
	
	private String content;
	
	private String filePath;
	
	private String fmtDelimited;
	
	private String titileContent;
	
	private Integer taskID;

	public Integer getTaskID() {
		return taskID;
	}

	public void setTaskID(Integer taskID) {
		this.taskID = taskID;
	}

	public String getTitileContent() {
		return titileContent;
	}

	public void setTitileContent(String titileContent) {
		this.titileContent = titileContent;
	}

	public String getFmtDelimited() {
		return fmtDelimited;
	}

	public void setFmtDelimited(String fmtDelimited) {
		this.fmtDelimited = fmtDelimited;
	}

	public String getFilePath() {
		return filePath;
	}

	public void setFilePath(String filePath) {
		this.filePath = filePath;
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

	public String getInsStatus() {
		return insStatus;
	}

	public void setInsStatus(String insStatus) {
		this.insStatus = insStatus;
	}

	public Integer getChildID() {
		return childID;
	}

	public void setChildID(Integer childID) {
		this.childID = childID;
	}
	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}

}
