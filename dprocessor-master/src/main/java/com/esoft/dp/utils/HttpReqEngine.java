package com.esoft.dp.utils;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpReqEngine {
	
	private static Logger logger = LoggerFactory
			.getLogger(HttpReqEngine.class);

	public static String sendReq(String reqURL) {
		String result = null;
		BufferedReader in = null;
		try {
			URL url = new URL(reqURL);
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
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
			logger.error("sendReq", e);
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
	/**
	 * @author taoshi
	 * post 方式请求rest 接口
	 * @param reqBody
	 * @return
	 */
	public static String sendPostReq(String reqURL, String reqBody) {
		String result = null;
		BufferedReader in = null;
		try {
			URL url = new URL(reqURL);
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("POST");
			conn.setRequestProperty("Accept-Charset", "utf-8");
			conn.setRequestProperty("contentType", "utf-8");
			conn.setDoOutput(true);
			conn.setDoInput(true);
			OutputStream out = conn.getOutputStream();
			out.write(reqBody.getBytes());
			out.close();
			int code = conn.getResponseCode();
			if (code >= 200 && code < 300) {
				return IoUtility.readStringFromInputStream(conn.getInputStream(), "utf-8");
			}
		} catch (Exception e) {
			logger.error("sendReq", e);
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
	
	public static String sendReBootReq(String reqUrl, Map<String,Object> params, String RequestMethod) throws IOException{
		BufferedReader in = null;
		try {
			URL url = new URL(reqUrl);
			HttpURLConnection con = (HttpURLConnection) url.openConnection();
			con.setRequestMethod(RequestMethod);//RequestMethod的值例如POST,GET,DELETE等等
			con.setRequestProperty("Accept-Charset", "utf-8");
			con.setRequestProperty("contentType", "application/x-www-form-urlencoded; charset=UTF-8");
			//设置参数
			if(params != null){
				Set<String> set = params.keySet();
				for(Iterator<String> iter = set.iterator(); iter.hasNext();){
					   String key = iter.next();
					   String value = params.get(key).toString();
					   con.setRequestProperty(key, value);
				}
			}
			
			con.setDoOutput(true);//是否输入参数
			con.setDoInput(true);
			int responseCode = con.getResponseCode();
			logger.info("Response code: " + responseCode);
			 in = new BufferedReader(new InputStreamReader(con.getInputStream(), "utf-8"));  
			 StringBuffer buffer = new StringBuffer();  
			 String line = "";  
			 while ((line = in.readLine()) != null){  
			     buffer.append(line);  
			 }  
			 logger.debug(buffer.toString());
			 return buffer.toString();
		
		} catch (Throwable e) {
			logger.error("",e);
			return "0";
		} finally{
			try {
				in.close();
			} catch (Exception e2) {
				logger.error("",e2);
			}
		}
	}
	
	public static String sendDelReq(String reqURL, String reqBody) {
		String result = null;
		BufferedReader in = null;
		try {
			HttpURLConnection conn = (HttpURLConnection) new URL(reqURL).openConnection();
			conn.setRequestMethod("DELETE");
			conn.setRequestProperty("Accept-Charset", "utf-8");
			conn.setRequestProperty("contentType", "utf-8");
			conn.setDoOutput(true);
			conn.setDoInput(true);
			OutputStream out = conn.getOutputStream();
			if(StringUtils.isNotBlank(reqBody)){
				out.write(reqBody.getBytes());	
			}
			int code = conn.getResponseCode();
			logger.info(new Integer(code).toString());
			if (code >= 200 && code < 300) {
				result =  IoUtility.readStringFromInputStream(conn.getInputStream(), "utf-8");
			}else{
				result =  IoUtility.readStringFromInputStream(conn.getErrorStream(), "utf-8");
			}
			out.close();
		} catch (Exception e) {
			logger.error("sendReq", e);
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
	
}