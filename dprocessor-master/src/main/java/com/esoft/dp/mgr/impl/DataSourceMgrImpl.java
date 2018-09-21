package com.esoft.dp.mgr.impl;

import org.quartz.JobExecutionContext;

import com.esoft.dp.mgr.TaskMgr;
/**
 * 
 * @author liuzd
 * 每10 秒扫描一次表中记录，发现 taskID 为 1 的记录，检查
 *
 */
public class DataSourceMgrImpl implements TaskMgr{

	@Override
	public void invoke(JobExecutionContext context) {
		// TODO Auto-generated method stub
		
	}

}
