package com.esoft.dp.mgr.impl;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.Proxy;
import java.net.URL;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esoft.dp.mgr.ConnectionMgr;
import com.esoft.dp.utils.IoUtility;


public class ConnectionMgrImpl implements ConnectionMgr{
	
	protected String proxyIP;
	
	protected String proxyPort;
	
	private static Logger logger = LoggerFactory.getLogger(ConnectionMgrImpl.class);
	

	@Override
	public HttpURLConnection prepareConnection(String url) throws MalformedURLException, IOException {
		Proxy proxy = null;
		if(StringUtils.isNotBlank(proxyIP)){
			proxy = new Proxy(Proxy.Type.HTTP,new InetSocketAddress(proxyIP,Integer.parseInt(proxyPort)));
			return (HttpURLConnection) new URL(url).openConnection(proxy);
		} else {
			return (HttpURLConnection) new URL(url).openConnection();
		}
	}
	
	public String sendReq(String url) {
		String result = null;
		BufferedReader in = null;
		try {
			HttpURLConnection conn = prepareConnection(url);
			conn.setRequestMethod("GET");
			conn.setRequestProperty("Accept-Charset", "utf-8");
			conn.setRequestProperty("contentType", "utf-8");
			conn.setDoOutput(true);
			conn.setDoInput(true);
			int code = conn.getResponseCode();
			if (code >= 200 && code < 300) {
				return IoUtility.readStringFromInputStream(conn.getInputStream(), "utf-8");
			}
		} catch (Exception e) {
			logger.error("",e);
			return null;
		} finally {
			if (in != null)
				try {
					in.close();
				} catch (IOException e) {
				}
		}
		// 返回
		return result;
	}
	public String getProxyIP() {
		return proxyIP;
	}

	public void setProxyIP(String proxyIP) {
		this.proxyIP = proxyIP;
	}

	public String getProxyPort() {
		return proxyPort;
	}

	public void setProxyPort(String proxyPort) {
		this.proxyPort = proxyPort;
	}

}
