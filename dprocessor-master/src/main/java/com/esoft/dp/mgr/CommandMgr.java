package com.esoft.dp.mgr;

import org.quartz.JobExecutionContext;

import com.esoft.dp.entity.Ancestor;

public interface CommandMgr {
	
	public void invoke(JobExecutionContext context);
	
	public void generateCommand(Ancestor t);

}
