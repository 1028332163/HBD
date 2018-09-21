//package com.esoft.dp.mgr.impl;
//import java.util.ArrayList;
//import java.util.List;
//
//import com.esoft.dp.mgr.SparkTaskMgr;
//public abstract class SubmitTaskMgrImpl implements SparkTaskMgr{
//	// -- master
//	private String master;
//	//deploy-mode
//	private String deployMode = "client";
//	//tarsk name
//	private String name = "TEST";
//	
//	private String className ="water.SparklingWaterDriver";
//	
//	private String out;
//	
//	private String in;
//	
//	private String executeMem = "2G";
//	
//	private String jarPath;
//
//	public String getJarPath() {
//		return jarPath;
//	}
//
//	public void setJarPath(String jarPath) {
//		this.jarPath = jarPath;
//	}
//
//	public String getMaster() {
//		return master;
//	}
//
//	public void setMaster(String master) {
//		this.master = master;
//	}
//
//	public String getDeployMode() {
//		return deployMode;
//	}
//
//	public void setDeployMode(String deployMode) {
//		this.deployMode = deployMode;
//	}
//
//	public String getName() {
//		return name;
//	}
//
//	public void setName(String name) {
//		this.name = name;
//	}
//
//	public String getClassName() {
//		return className;
//	}
//
//	public void setClassName(String className) {
//		this.className = className;
//	}
//
//	public String getOut() {
//		return out;
//	}
//
//	public void setOut(String out) {
//		this.out = out;
//	}
//
//	public String getIn() {
//		return in;
//	}
//
//	public void setIn(String in) {
//		this.in = in;
//	}
//	
//	public String getExecuteMem() {
//		return executeMem;
//	}
//
//	public void setExecuteMem(String executeMem) {
//		this.executeMem = executeMem;
//	}
//
//	@Override
//	public List<String> prepare() {
//		 List<String> args = new ArrayList<String>();
//		// args.add("--master");
//		// args.add(getMaster());
//		 args.add("--deploy-mode");
//		 args.add(getDeployMode());
//		 args.add("--name");
//		 args.add(getName());
//		 args.add("--class");
//		 args.add(getClassName());
//		 args.add("--executor-memory");
//		 args.add(getExecuteMem());
//		 return args;
//	}
//
//}
