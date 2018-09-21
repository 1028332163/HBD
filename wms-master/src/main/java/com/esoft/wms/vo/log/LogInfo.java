package com.esoft.wms.vo.log;
/**
 * 
 * @author taos
 * 查看日志功能
 */
public class LogInfo {
	//偏移量
	private String offset;
	//获取到要查询的日志名
	private String fileName;
	//获取未读取整行的日志
	private String noAllLog;
	//获取 jobid
	private String jobid;
	//区分默认读取的还是点击查看之前日志
	private String mark;
	
	private String extraMessage;
	//日志级别
	private String severity;

	//获取有关jobid所有信息
	private String message;

	//日志类别 0，jobserver 1，定时任务
	private String logType;

	private String logDataLen;
	public String getLogType() {
		return logType;
	}
	public void setLogType(String logType) {
		this.logType = logType;
	}
	public String getSeverity() {
		return severity;
	}
	public void setSeverity(String severity) {
		this.severity = severity;
	}
	public String getExtraMessage() {
		return extraMessage;
	}
	public void setExtraMessage(String extraMessage) {
		this.extraMessage = extraMessage;
	}
	public String getOffset() {
		return offset;
	}
	public void setOffset(String offset) {
		this.offset = offset;
	}
	public String getFileName() {
		return fileName;
	}
	public void setFileName(String fileName) {
		this.fileName = fileName;
	}
	public String getNoAllLog() {
		return noAllLog;
	}
	public void setNoAllLog(String noAllLog) {
		this.noAllLog = noAllLog;
	}
	public String getJobid() {
		return jobid;
	}
	public void setJobid(String jobid) {
		this.jobid = jobid;
	}
	public String getMark() {
		return mark;
	}
	public void setMark(String mark) {
		this.mark = mark;
	}
	public String getMessage() {
		return message;
	}
	public void setMessage(String message) {
		this.message = message;
	}
	public String getLogDataLen() {
		return logDataLen;
	}
	public void setLogDataLen(String logDataLen) {
		this.logDataLen = logDataLen;
	}
	
}
