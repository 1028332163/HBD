package com.esoft.dp.mgr.impl;

import com.esoft.dp.entity.Ancestor;
import com.esoft.dp.entity.Task;
import com.esoft.dp.utils.HttpReqEngine;

/**
 * 
 * @author taoshi
 * 通过jobserver 提交spark 任务
 *
 */
public class JobServerSubmitCommandMgrImpl extends CommandExecMgrImpl{
	//调用spark job server 的接口地址
	private String reqURL;

	private HttpReqEngine reqEngine;

	@Override
	public void generateCommand(Ancestor t) {
		// TODO Auto-generated method stub
		//拼接调用的url
		Task task = (Task)t;
		String content = reqEngine.sendPostReq(reqURL, task.getSparkCmdContent());
	}
	
	public void setReqEngine(HttpReqEngine reqEngine) {
		this.reqEngine = reqEngine;
	}
	public void setReqURL(String reqURL) {
		this.reqURL = reqURL;
	}
	


}
