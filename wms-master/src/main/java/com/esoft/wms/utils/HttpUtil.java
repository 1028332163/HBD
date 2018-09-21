package com.esoft.wms.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * 
 * @author taoshi
 * 发送http 请求 的工具类，支持post 和 get 请求
 *
 */
public class HttpUtil {
	
	private HttpUtil(){}
	
	private static Logger logger = LoggerFactory
			.getLogger(HttpUtil.class);
	
	public static String sendReq(String reqURL) {
		String result = null;
		BufferedReader in = null;
		try {
			HttpURLConnection conn = (HttpURLConnection) new URL(reqURL).openConnection();
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
			HttpURLConnection conn = (HttpURLConnection) new URL(reqURL).openConnection();
			conn.setRequestMethod("POST");
			conn.setRequestProperty("Accept-Charset", "utf-8");
			conn.setRequestProperty("contentType", "utf-8");
			conn.setDoOutput(true);
			conn.setDoInput(true);
			OutputStream out = conn.getOutputStream();
			out.write(reqBody.getBytes());			
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
