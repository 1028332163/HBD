package com.esoft.dp.mgr;

import java.util.List;

import org.quartz.JobExecutionContext;

public interface SparkTaskMgr {
	
	void execute(JobExecutionContext context);
	
	List<String> prepare();

}
