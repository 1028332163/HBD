package com.esoft.dp.mgr.impl;

import java.io.IOException;

import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esoft.dp.mgr.JobserverMgr;
import com.esoft.dp.utils.HttpReqEngine;

public class JobServerRebootMgrImpl implements JobserverMgr{
	
	private static Logger logger = LoggerFactory
			.getLogger(JobServerRebootMgrImpl.class);
	
	//jobserver地址
	private String jobServerUrl;
	
	@Override
	public void invoke(JobExecutionContext context) throws IOException {
		logger.info("JobServerReboot");
		
		String jobServerRebootUrl = jobServerUrl + "/contexts?reset=reboot";
		String RequestMethod = "PUT";
		
		logger.info("run:{}","curl -X PUT"+jobServerUrl + "/contexts?reset=reboot");
		String content = HttpReqEngine.sendReBootReq(jobServerRebootUrl,null,RequestMethod);//执行发送reboot context
		
		logger.info("jobserver context reboot :{}",content);
	}

	public void setJobServerUrl(String jobServerUrl) {
		this.jobServerUrl = jobServerUrl;
	}
	
}
