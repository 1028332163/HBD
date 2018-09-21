package com.esoft.dp.entity;

import java.io.Serializable;
import java.sql.Timestamp;

public class OrigialDataSet implements Serializable {
	
	private static final long serialVersionUID = -2875912996835503724L;
	
	private Integer id;
	
	private String name;
	
	private String filePath;
	
	private Timestamp createTime;
	
	private String originalType;
	
	private String titileContent;
	
	private String datatableColunmsLongtext;
	
	private String theadLongtext;

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

	public String getFilePath() {
		return filePath;
	}

	public void setFilePath(String filePath) {
		this.filePath = filePath;
	}

	public Timestamp getCreateTime() {
		return createTime;
	}

	public void setCreateTime(Timestamp createTime) {
		this.createTime = createTime;
	}

	public String getOriginalType() {
		return originalType;
	}

	public void setOriginalType(String originalType) {
		this.originalType = originalType;
	}

	public String getTitileContent() {
		return titileContent;
	}

	public void setTitileContent(String titileContent) {
		this.titileContent = titileContent;
	}

	public String getDatatableColunmsLongtext() {
		return datatableColunmsLongtext;
	}

	public void setDatatableColunmsLongtext(String datatableColunmsLongtext) {
		this.datatableColunmsLongtext = datatableColunmsLongtext;
	}

	public String getTheadLongtext() {
		return theadLongtext;
	}

	public void setTheadLongtext(String theadLongtext) {
		this.theadLongtext = theadLongtext;
	}

}
