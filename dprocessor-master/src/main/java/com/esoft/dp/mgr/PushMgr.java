package com.esoft.dp.mgr;

import java.util.List;

import org.quartz.JobExecutionContext;

/**
 * 
 * @author taoshi
 * 控制推送逻辑
 *
 */
public interface PushMgr {
	
	public List<?> prepare();
	
	public void invoke(JobExecutionContext context);

}
