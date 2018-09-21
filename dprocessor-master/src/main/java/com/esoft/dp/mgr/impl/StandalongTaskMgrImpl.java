//package com.esoft.dp.mgr.impl;
//
//import java.util.List;
//
//import org.apache.spark.deploy.SparkSubmit;
//import org.quartz.JobExecutionContext;
//
//public class StandalongTaskMgrImpl extends SubmitTaskMgrImpl{
//	
//	public List<String> prepare(){
//		return super.prepare();
//	}
//	
//	public void execute(JobExecutionContext context){
//		List<String> arg0 = prepare();
//	    SparkSubmit.main(arg0.toArray(new String[arg0.size()])); 
//	}
//
//}
