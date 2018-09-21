package com.esoft.dp.mgr.impl;


import java.io.File;

import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esoft.dp.mgr.JobserverMgr;
import com.esoft.dp.utils.ExecUtil;


public class JobServerOnOffMgrImpl implements JobserverMgr{

	private static Logger logger = LoggerFactory
			.getLogger(JobServerOnOffMgrImpl.class);
	
	private String jobServerOn;//jobserver启动shell路径
	private String jobServerOff;//jobserver停止shell路径
	private String jobServerPid;//jobserver下的pid文件的路径
	
	@Override
	public void invoke(JobExecutionContext context)  {
		logger.info("JobServerOnOff");
		
		//执行jobserver服务停止命令得到stop的输出内容
		logger.info("run:{}",jobServerOff);
		String exitValue = ExecUtil.exec(jobServerOff);
		logger.info("run jobServerOff:{}", exitValue);
		
		File file=new File(jobServerPid);   
		
		//判断执行停止jobserver服务后pid文件是否存在，若存在先删除
		if (exitValue.contains("stopped")) {
			logger.info("run:{}",jobServerOn);
			ExecUtil.exec(jobServerOn);
		} else {
			if(file.exists()) {  
				//执行jobserver目录下的pid文件命令
				logger.info("run:{}","rm -f "+jobServerPid);
				ExecUtil.exec("rm -f "+jobServerPid); 
				//执行jobserver服务启动命令
				logger.info("run:{}",jobServerOn);
			    ExecUtil.exec(jobServerOn);
			} else {
				logger.info("run:{}",jobServerOn);
				ExecUtil.exec(jobServerOn);
			}
		}
	}
	
	public void setJobServerOn(String jobServerOn) {
		this.jobServerOn = jobServerOn;
	}

	public void setJobServerOff(String jobServerOff) {
		this.jobServerOff = jobServerOff;
	}

	public void setJobServerPid(String jobServerPid) {
		this.jobServerPid = jobServerPid;
	}
	

}
