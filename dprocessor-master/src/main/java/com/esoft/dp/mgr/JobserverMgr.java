package com.esoft.dp.mgr;

import java.io.IOException;

import org.quartz.JobExecutionContext;

public interface JobserverMgr {
	
	public void invoke(JobExecutionContext context) throws IOException;

}
