package com.esoft.wms.vo;
/**
 * 
 * @author taoshi
 * 调优结果反馈
 *
 */
public class TurningResult {
	

	@Override
	public String toString() {
		return "TurningResult [succ=" + succ + ", contextName=" + contextName
				+ ", errorMsg=" + errorMsg + "]";
	}

	private boolean succ;

	private String contextName;
	
	private String errorMsg;
	
	public boolean isSucc() {
		return succ;
	}

	public void setSucc(boolean succ) {
		this.succ = succ;
	}

	public String getContextName() {
		return contextName;
	}

	public void setContextName(String contextName) {
		this.contextName = contextName;
	}

	public String getErrorMsg() {
		return errorMsg;
	}

	public void setErrorMsg(String errorMsg) {
		this.errorMsg = errorMsg;
	}


}
