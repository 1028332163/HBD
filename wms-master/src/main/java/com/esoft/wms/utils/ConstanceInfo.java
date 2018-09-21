package com.esoft.wms.utils;

public class ConstanceInfo {
	
	public static final String TASK_INSTANCE_STATUS_NEW = "start";
	
	public static final String TASK_INSTANCE_STATUS_SPARK = "sparking";
	
	public static final String TASK_INSTANCE_STATUS_SPARKED = "sparked";
	
	public static final String TASK_INSTANCE_STATUS_SPARKFAILED = "sparkfal";
	
	public static final String TASK_INSTANCE_STATUS_HIVING = "hiving";
	
	public static final String TASK_INSTANCE_STATUS_HIVEFAL = "hivefal";
	
	public static final String TASK_INSTANCE_STATUS_HIVED = "hived";
	
	public static final String TASK_INSTANCE_STATUS_END = "end";
	
	
	
	//文件类型
	
	public static final String TASK_FILE_TYPE_PARQUET = ".parquet";
	
	public static final String TASK_FILE_TYPE_ORC = ".orc";
	
	//默认 instance_slave status 是0
	
	public static final int DEFAULT_TASK_SLAVE_STATUS = 0;
	
	public static final int DEFAULT_TASK_SLAVE_PUSH_TIMES = 0;
	
	//project status
	public static final String PROJECT_STATUS_START = "start";
	
	public static final String PROJECT_STATUS_END = "end";
	
	public static final String PROJECT_STATUS_FAIL = "failed";
	
	public static final String PROJECT_STATUS4PUSH_END = "projectEnd";
	
	//jobServer的http请求结果
	public static final String JOB_SEND_FULL = "full";
	
	//日志类别
	public static final String JOBSERVER = "0";
	
	public static final String SIMPLE = "1";
	
	public static final String JOBSERVER_NO_RES = "NO_RES";
	
	//job server 返回的status
	public static final String STARTED_JOBSERVER = "STARTED";
	
	public static final String INIT_SUCC_JOBSERVER = "SUCCESS";
	
	public static final String ERROR_JOBSERVER = "ERROR";
	
	public static final String FAIL_JOBSERVER = "fail";
	
	public static final String LOADING_FAILED_JOBSERVER = "JOB LOADING FAILED";
	
	public static final String TOO_MANY_JOBD = "NO SLOTS AVAILABLE";
	
	//system
	public static final String fileSeperator = "/";
	
	public static final String tableNamePrefix = "p";
	
	public static final String tableNameSep = "_";
	
	//调用接口信息
	public static final String CONFIG_PARAMS_REQ = "配置参数缺失";
	//job Server no res
	public static final String JOB_SERVER_NO_RESPONSE = "The server was not able to produce a timely response to your request.";
	
	public static final String KILL_CONTEXT_RES_NO_CONTEXT = "NO CONTEXT!";
	
	public static final String KILL_CONTEXT_RES_FAIL = "FAILED!";
	
	public static final String KILL_CONTEXT_SUCC = "SUCC!";
	
	public static final String NO_TASK_HIVED = "NO_TASK_HIVED";

	public static final String JOB_SERVER_NO_START = "NO_RES";
	
	public static final String DATASOUCE_TASK_NAME = "DataBase";
	
}
