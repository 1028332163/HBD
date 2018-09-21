package com.esoft.dp.mgr.impl;

import com.esoft.dp.dao.DaoUtility;
import com.esoft.dp.entity.Task;
import com.esoft.dp.mgr.TaskMgr;

public abstract class HiveTaskMgr implements TaskMgr{
	
	protected String hadoopFSDir;	
	protected DaoUtility daoUtility;
	protected String hiveDbName;
	
	public String getHiveDbName() {
		return hiveDbName;
	}

	public void setHiveDbName(String hiveDbName) {
		this.hiveDbName = hiveDbName;
	}
	public abstract boolean prepare(Object obj,Task instance);
	
	public abstract void add(Task task);
	
	public abstract void update(Object t,String beginStatus,String endStatus, String errorHint);
	
	
//	public void invoke(JobExecutionContext context){
//		
//		//查询新建的任务 0:新建；1：Spark 任务OK
//		List<?> result = daoUtility
//				.findListHql("from TarskInstance ti where ti.status = '0' or ti.status = '1'");
//		
//		for (Object object : result) {
//			TaskInstance instance = (TaskInstance) object;
//			if(ConstanceInfo.TASK_INSTANCE_STATUS_NEW
//								.equals(instance.getInsStatus())){
//				//project id + tarsk id + instance id
//				add();
//			} else {
//				update(instance.getId());
//			}
//			
//		}
//	}
	
	public void setDaoUtility(DaoUtility daoUtility) {
		this.daoUtility = daoUtility;
	}
	
	public String getHadoopFSDir() {
		return hadoopFSDir;
	}

	public void setHadoopFSDir(String hadoopFSDir) {
		this.hadoopFSDir = hadoopFSDir;
	}

}
