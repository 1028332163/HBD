package com.esoft.dp.vo;

public class ConstanceInfo {
	
	public static final String TASK_INSTANCE_STATUS_NEW = "start";
	
	public static final String TASK_INSTANCE_STATUS_SPARK = "sparking";
	
	public static final String TASK_INSTANCE_STATUS_SPARKED = "sparked";
	
	public static final String TASK_INSTANCE_STATUS_SPARKFAILED = "sparkfal";
	
	public static final String TASK_INSTANCE_STATUS_HIVING = "hiving";
	
	public static final String TASK_INSTANCE_STATUS_HIVEFAL = "hivefal";
	
	public static final String TASK_INSTANCE_STATUS_HIVED = "hived";
	
	public static final String TASK_INSTANCE_STATUS_END = "end";
	
	public static final String TASK_INSTANCE_STATUS_GENERATED = "1";
	
	public static final String TASK_INSTANCE_STATUS_OK = "2";
	
	public static final String TASK_INSTANCE_STATUS_ERROR = "9";
	
	//instance data 附属表状态
	public static final String TASK_INSTANCE_DATA_STATUS_NEW = "sparked";
	
	public static final String TASK_INSTANCE_DATA_STATUS_HIVING = "hiving";
	
	public static final String TASK_INSTANCE_DATA_STATUS_OK = "hived";
	
	public static final String TASK_INSTANCE_DATA_STATUS_FAIL = "hivefal";
	//记录推送状态
	public static final String TASK_INSTANCE_SLAVE_STATUS_UNPUSHED = "0";
	
	public static final String TASK_INSTANCE_SLAVE_STATUS_PUSHED = "1";
	
	public static final String TASK_INSTANCE_SLAVE_STATUS_RECEIVED = "2";
	
	//jobServer的http请求结果
	public static final String JOB_SEND_FULL = "full";
	

}
