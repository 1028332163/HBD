//package com.esoft.dp.mgr.impl;
//import java.util.List;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.spark.SparkConf;
//import org.apache.spark.deploy.yarn.Client;
//import org.apache.spark.deploy.yarn.ClientArguments;
//import org.quartz.JobExecutionContext;
//
//public class YarnTarskMgrImpl extends SubmitTaskMgrImpl{
//	
//	private String mapreduceFrName;
//	
//	private String yarnRsmAddress;
//	
//	private String jobHisAddress;
//	
//	private String defaultFS;
//	
//	private String addJars;
//
//	public List<String> prepare() {
//		List<String> arg0 = super.prepare();
//		arg0.add("--jar");
//		String tmp = Thread.currentThread().getContextClassLoader().getResource("").getPath();
//		tmp = tmp.substring(0, tmp.length() - 8); 
//		arg0.add(tmp + "lib/sparkling-water-assembly-1.5.6-all.jar");
//		//arg0.add("--arg");
//		//arg0.add(getIn());
//		//arg0.add("--arg");
//		//arg0.add(getOut());
//		return arg0;
//	}
//	
//	@Override
//	public void execute(JobExecutionContext context) {
//		//String tmp = Thread.currentThread().getContextClassLoader().getResource("").getPath();
//		String[] arg0=new String[]{  
//                "--name","pang",  
//                "--class","water.SparklingWaterDriver",  
//                "--executor-memory","1G",
//                //"--num-executors","2",
////              "WebRoot/WEB-INF/lib/spark_filter.jar",//  
//                "--jar","lib/sparkling-water-assembly-1.5.6-all.jar"//  
//                  
////                "--arg","hdfs://idh104:8020/user/root/log.txt",  
////                "--arg","hdfs://node101:8020/user/root/badLines_yarn_"+"2.txt",  
////                "--addJars","hdfs://node101:8020/user/root/servlet-api.jar",//  
////                "--archives","hdfs://node101:8020/user/root/servlet-api.jar"//  
//        };  
//		//List<String> arg0 = prepare();
//		Configuration conf = new Configuration();
//		conf.setBoolean("mapreduce.app-submission.cross-platform", getOS());// 配置使用跨平台提交任务
//		conf.set("fs.defaultFS", "hdfs://idh104:8020");// 指定namenode
//		conf.set("mapreduce.framework.name","yarn"); // 指定使用yarn框架
//		conf.set("yarn.resourcemanager.address","idh104:8050"); // 指定resourcemanager
//		conf.set("yarn.resourcemanager.scheduler.address", "idh104:8030");// 指定资源分配器
//		conf.set("yarn.resourcemanager.admin.address", "idh104:8141");
//		conf.set("yarn.resourcemanager.resource-tracker.address", "idh104:8025");
////		sparkConf.set("mapreduce.jobtracker.address", "192.168.1.10:9001");
////        sparkConf.set("yarn.resourcemanager.admin.address", "192.168.1.10:8033");
////        sparkConf.set("yarn.resourcemanager.resource-tracker.address", "192.168.1.10:8031");
////        sparkConf.set("yarn.resourcemanager.hostname", "192.168.1.10");
//		
//		
//		conf.set("mapreduce.jobhistory.address","idh105:10020");
//		System.setProperty("SPARK_YARN_MODE", "true");
//		SparkConf sparkConf = new SparkConf();
//		//sparkConf.setMaster("yarn-cluster");
//		ClientArguments cArgs = new ClientArguments(arg0,sparkConf);
//		Client c = new Client(cArgs,conf,sparkConf);
//		c.run();
//		
//	}
//	
//	public String getAddJars() {
//		return addJars;
//	}
//
//	public void setAddJars(String addJars) {
//		this.addJars = addJars;
//	}
//	
//	public boolean getOS(){
//		String os = System.getProperty("os.name");
//		
//		boolean cross_platform =false;
//		
//		if(os.contains("Windows"))cross_platform = true;
//		
//		return cross_platform;
//	}
//	
//	public String getMapreduceFrName() {
//		return mapreduceFrName;
//	}
//
//	public void setMapreduceFrName(String mapreduceFrName) {
//		this.mapreduceFrName = mapreduceFrName;
//	}
//
//	public String getYarnRsmAddress() {
//		return yarnRsmAddress;
//	}
//
//	public void setYarnRsmAddress(String yarnRsmAddress) {
//		this.yarnRsmAddress = yarnRsmAddress;
//	}
//
//	public String getJobHisAddress() {
//		return jobHisAddress;
//	}
//
//	public void setJobHisAddress(String jobHisAddress) {
//		this.jobHisAddress = jobHisAddress;
//	}
//	public String getDefaultFS() {
//		return defaultFS;
//	}
//
//	public void setDefaultFS(String defaultFS) {
//		this.defaultFS = defaultFS;
//	}
//
//}
