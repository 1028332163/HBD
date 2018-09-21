package com.esoft.dp.mgr;

import org.quartz.JobExecutionContext;

public interface TaskMgr {
	
	
	public void invoke(JobExecutionContext context);

}
