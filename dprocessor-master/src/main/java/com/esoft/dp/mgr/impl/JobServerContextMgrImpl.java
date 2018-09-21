package com.esoft.dp.mgr.impl;

import java.io.IOException;

import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esoft.dp.mgr.JobserverMgr;
import com.esoft.dp.utils.HttpReqEngine;

public class JobServerContextMgrImpl implements JobserverMgr {
	
	private static Logger logger = LoggerFactory
			.getLogger(JobServerOnOffMgrImpl.class);
	
	private String jobServerUrl;//jobserver地址
	
	@Override
	public void invoke(JobExecutionContext context) throws IOException  {
		logger.info("killContext");
		
		//拼接要发送的jobserver的http地址  my-low-latency-context是context名
		String killContextURL = generateKillContextURL("my-low-latency-context");
		String RequestMethod = "DELETE";
		//执行发送kill context
		logger.info("run:{}","curl -X DELETE "+killContextURL);
		String content = HttpReqEngine.sendReBootReq(killContextURL,null,RequestMethod);
		
		logger.info(content);
	}
	
	public  String generateKillContextURL(String contextName) {
		StringBuilder sb = new StringBuilder(jobServerUrl);
		sb.append("/contexts/");
		sb.append(contextName);
		
		return sb.toString();
	}
	
	public void setJobServerUrl(String jobServerUrl) {
		this.jobServerUrl = jobServerUrl;
	}

}
