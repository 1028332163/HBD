package com.esoft.dp.entity;

import java.io.Serializable;
import org.springframework.util.StringUtils;
import com.esoft.dp.vo.ConstanceInfo;

/**
 * 记录正在执行的任务
 * 
 * @author taosh
 *
 */
public class Task extends Ancestor implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -2875912996835503724L;
	
	private TaskInfo taskInfo;
	
	private Integer id;
	
	private String name;
	
	private String insStatus = ConstanceInfo.TASK_INSTANCE_STATUS_NEW;
	
	private String parentID;
	
	private String tableName;
	
	private String titleContent;
	
	private String uuid;
	
	private String fmtDelimited;
	
	//数据源所引用的数据集ID
	private Integer datasetID;
	//工程ID
	private Integer projectID;
	
	private String datasourceFile;
	
	private String sparkResult;
	
	private String sparkCmdContent;
	
	private String paramJsonContent;
	
	private String jobSendResult;
	
	public String getJobSendResult() {
		return jobSendResult;
	}

	public void setJobSendResult(String jobSendResult) {
		this.jobSendResult = jobSendResult;
	}

	public String getParamJsonContent() {
		return paramJsonContent;
	}

	public void setParamJsonContent(String paramJsonContent) {
		this.paramJsonContent = paramJsonContent;
	}

	public String getSparkCmdContent() {
		return sparkCmdContent;
	}

	public void setSparkCmdContent(String sparkCmdContent) {
		this.sparkCmdContent = sparkCmdContent;
	}

	public TaskInfo getTaskInfo() {
		return taskInfo;
	}

	public void setTaskInfo(TaskInfo taskInfo) {
		this.taskInfo = taskInfo;
	}
	
	public String getDatasourceFile() {
		return datasourceFile;
	}

	public void setDatasourceFile(String datasourceFile) {
		this.datasourceFile = datasourceFile;
	}

	public String getDsID() {
		return dsID;
	}
	
	public String getSparkResult() {
		return sparkResult;
	}

	public void setSparkResult(String sparkResult) {
		this.sparkResult = sparkResult;
	}

	public void setDsID(String dsID) {
		if(!StringUtils.isEmpty(dsID)){
			this.datasetID = Integer.parseInt(dsID);
		}
		this.dsID = dsID;
	}

	private String dsID;


	public String getFmtDelimited() {
		return fmtDelimited;
	}

	public void setFmtDelimited(String fmtDelimited) {
		this.fmtDelimited = fmtDelimited;
	}

	public String getUuid() {
		return uuid;
	}

	public void setUuid(String uuid) {
		this.uuid = uuid;
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

	public String getParentID() {
		return parentID;
	}

	public void setParentID(String parentID) {
		this.parentID = parentID;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getTitleContent() {
		return titleContent;
	}

	public void setTitleContent(String titleContent) {
		this.titleContent = titleContent;
	}

	public Integer getDatasetID() {
		return datasetID;
	}

	public void setDatasetID(Integer datasetID) {
		this.datasetID = datasetID;
	}

	public Integer getProjectID() {
		return projectID;
	}

	public void setProjectID(Integer projectID) {
		this.projectID = projectID;
	}
	
}
