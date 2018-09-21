package com.esoft.dp.entity;

import java.io.Serializable;
import com.esoft.dp.vo.ConstanceInfo;
/**
 * 
 * @author taoshi
 * 算法执行过程中的中间数据，
 * 存储到新的parquet 文件，并建立hive 表
 */
public class TaskInstanceData extends Ancestor implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 5553539228579911714L;

	private Integer id;
	
	private String name;
	//状态字段，是否成功建立hive 表并写入
	private String hiveStatus = ConstanceInfo.TASK_INSTANCE_STATUS_NEW;
	//建立hive 表的表明
	private String tableName;
	//右键菜单
	private String menuType;
	
	private String taskID;
	
	private String titleContent;
	//生成parquet 文件的路径
	private String sparkResult;
	//hive表头 
	private String summaryJsonContent;
	
	private String datatableColumnsLongtext;
	
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

	public String getHiveStatus() {
		return hiveStatus;
	}

	public void setHiveStatus(String hiveStatus) {
		this.hiveStatus = hiveStatus;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getMenuType() {
		return menuType;
	}

	public void setMenuType(String menuType) {
		this.menuType = menuType;
	}

	public String getTaskID() {
		return taskID;
	}

	public void setTaskID(String taskID) {
		this.taskID = taskID;
	}

	public String getSparkResult() {
		return sparkResult;
	}

	public void setSparkResult(String sparkResult) {
		this.sparkResult = sparkResult;
	}

	public String getSummaryJsonContent() {
		return summaryJsonContent;
	}

	public void setSummaryJsonContent(String summaryJsonContent) {
		this.summaryJsonContent = summaryJsonContent;
	}

	public String getTitleContent() {
		return titleContent;
	}

	public void setTitleContent(String titleContent) {
		this.titleContent = titleContent;
	}

	public String getDatatableColumnsLongtext() {
		return datatableColumnsLongtext;
	}

	public void setDatatableColumnsLongtext(String datatableColumnsLongtext) {
		this.datatableColumnsLongtext = datatableColumnsLongtext;
	}


}
