package com.esoft.dp.utils.enums;

public enum RetError {
	
	HAS_NO_PARAMS("0", "没有参数"),
	READ_LINE_ERROR("1", "读取参数失败"),
    PASSWORD_ERROR("2", "密码错误");
	
	private String code;
	
    private String msg;
    
    private RetError(String code, String msg) {
        this.code = code;
        this.msg = msg;
    }
    
    public String getCode() {
        return code;
    }
    
    public String getMsg() {
        return msg;
    }

}
