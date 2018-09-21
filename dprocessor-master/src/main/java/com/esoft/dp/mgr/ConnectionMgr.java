package com.esoft.dp.mgr;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;

public interface ConnectionMgr {
	
	public HttpURLConnection prepareConnection(String url) throws MalformedURLException, IOException;
	
	public String sendReq(String url);

}
