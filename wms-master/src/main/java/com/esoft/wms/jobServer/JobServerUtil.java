package com.esoft.wms.jobServer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

import org.codehaus.plexus.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esoft.wms.entity.TaskInstance;
import com.esoft.wms.utils.Config;
import com.esoft.wms.utils.ConstanceInfo;
import com.esoft.wms.utils.IoUtility;

public class JobServerUtil {
	private static Logger logger = LoggerFactory.getLogger(JobServerUtil.class);
	// private static String PREFIX = "input.string = \"";
	private static final String SEP = "-=-";
	private static final String EQUALS = "=";
	private static final String URL_SEP = "&";
	// 资源配置
	private static final String amMem = "spark.yarn.am.memory";
	private static final String amCores = "spark.yarn.am.cores";
	private static final String executorMem = "spark.executor.memory";
	private static final String executorCores = "spark.executor.cores";
	private static final String executorInstance = "spark.executor.instances";
	private static final String sparkQueue = "--queue";
	// private static final String suffix = "\"";
	private static final String hadoopFS = Config.props
			.getProperty("commandUtils.hadoopFS");

	// private static final String hadoopFS = "hdfs://192.168.88.10:8020/";

	public static String generateReqBody(TaskInstance taskInstance) {
		StringBuilder sb = new StringBuilder();
		
		sb.append(hadoopFS);
		sb.append(SEP);
		sb.append(taskInstance.getAlArg());
		sb.append(SEP);
		sb.append(taskInstance.getId());
		
		return sb.toString();
	}

	private static final String ip = Config.props.getProperty("jobServer.ip");
	private static final String port = Config.props
			.getProperty("jobServer.port");
	private static final String appName = Config.props
			.getProperty("jobServer.appName");
	private static final String context = Config.props
			.getProperty("jobServer.context");
	// 执行调优jobServer 的地址
	private static final String extIP = Config.props
			.getProperty("jobServer.extip");
	// 执行调优jobServer端口
	private static final String extPort = Config.props
			.getProperty("jobServer.extport");
	// 默认的DriverCore
	private static final String defaultDriverCores = Config.props
			.getProperty("jobServer.defaultDriverCores");
	// 默认ExecutorCores
	private static final String defaultExecutorCores = Config.props
			.getProperty("jobServer.executor.cores");
	// 默认调优的Queue
	private static final String extQueue = Config.props
			.getProperty("jobServer.extQueue");

	/**
	 * @author taosh 利用调优的context
	 * @param taskClass
	 * @param context
	 * @return
	 */
	public static String generateReqURL(String taskClass, String contextName) {
		// TODO Auto-generated method stub
		// 'localhost:8090/jobs?appName=liuzw&classPath=com.esoft.dae.model.kmeansCluster&context=test&sync=false'
		StringBuilder sb = null;
		// 是否默认context
		if (StringUtils.isBlank(contextName)) {
			sb = new StringBuilder(ip);
			sb.append(":");
			sb.append(port);
			sb.append("/jobs");
			sb.append("?appName=");
			sb.append(appName);
			sb.append("&classPath=");
			sb.append(taskClass.replace("com.esoft.dae",
					"com.esoft.dae.jobserver"));
			sb.append("&context=");
			sb.append(context);
		} else {
			sb = new StringBuilder(extIP);
			sb.append(":");
			sb.append(extPort);
			sb.append("/jobs");
			sb.append("?appName=");
			sb.append(appName);
			sb.append("&classPath=");
			sb.append(taskClass.replace("com.esoft.dae",
					"com.esoft.dae.jobserver"));
			sb.append("&context=");
			sb.append(contextName);
		}
		sb.append("&sync=false");
		return sb.toString();
	}

	/**
	 * @author taosh 创建调优的 context
	 * @param taskClass
	 * @param taskInstance
	 * @return
	 */
	public static String generateContextURL(String taskClass, TaskInstance taskInstance,
			String contextName) {
		// TODO Auto-generated method stub
		// curl -d ""
		// 'localhost:8090/contexts/context1-104?spark.yarn.am.memory=4G&spark.yarn.am.cores=2&spark.executor.memory=2G&spark.executor.cores=3&spark.executor.instances=3'
		StringBuilder sb = new StringBuilder(
				generateKillContextURL(contextName));
		sb.append("?");

		// am memory
		sb.append(amMem);
		sb.append(EQUALS);
		sb.append(taskInstance.getDriverMem());
		sb.append(URL_SEP);

		sb.append(amCores);
		sb.append(EQUALS);
		sb.append(defaultDriverCores);
		sb.append(URL_SEP);

		sb.append(executorCores);
		sb.append(EQUALS);
		sb.append(defaultExecutorCores);
		sb.append(URL_SEP);

		sb.append(executorMem);
		sb.append(EQUALS);
		sb.append(taskInstance.getExecutorMem());
		sb.append(URL_SEP);
		// executor number
		sb.append(executorInstance);
		sb.append(EQUALS);
		sb.append(taskInstance.getNumExecutors());
		// & queue=spark
		if (StringUtils.isNotBlank(extQueue)) {
			sb.append(URL_SEP);
			sb.append(sparkQueue);
			sb.append(extQueue);
		}
		logger.info("generateContextURL:{}", sb.toString());
		return sb.toString();
	}

	/**
	 * @author taos 删除context url
	 * @param contextName
	 * @return
	 */
	public static String generateKillContextURL(String contextName) {
		// TODO Auto-generated method stub
		// DELETE /contexts/<name> - stops a context and all jobs running in it
		StringBuilder sb = new StringBuilder(extIP);
		sb.append(":");
		sb.append(extPort);
		sb.append("/contexts/");
		sb.append(contextName);
		logger.info("generateKillContextURL:{}", sb.toString());
		return sb.toString();
	}

	/**
	 * @author taoshi post 方式请求rest 接口
	 * @param reqBody
	 * @return
	 */
	public static String sendPostReq(String reqURL, String reqBody) {
		String result = null;
		BufferedReader in = null;
		try {
			HttpURLConnection conn = (HttpURLConnection) new URL(reqURL)
					.openConnection();
			conn.setRequestMethod("POST");
			conn.setRequestProperty("Accept-Charset", "utf-8");
			conn.setRequestProperty("contentType", "utf-8");
			conn.setDoOutput(true);
			conn.setDoInput(true);
			OutputStream out = conn.getOutputStream();
			if (StringUtils.isNotBlank(reqBody)) {
				out.write(reqBody.getBytes());
			}
			int code = conn.getResponseCode();
			if (code >= 200 && code < 300) {
				result = IoUtility.readStringFromInputStream(
						conn.getInputStream(), "utf-8");
			} else {
				result = IoUtility.readStringFromInputStream(
						conn.getErrorStream(), "utf-8");
			}
			out.close();
		} catch (Exception e) {
			logger.error("sendReq", e);
			return ConstanceInfo.JOBSERVER_NO_RES;
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
	 * @author taoshi put 方式请求rest 接口
	 * @param reqBody
	 * @return
	 */
	public static String sendDelReq(String reqURL, String reqBody) {
		String result = null;
		BufferedReader in = null;
		try {
			HttpURLConnection conn = (HttpURLConnection) new URL(reqURL)
					.openConnection();
			conn.setRequestMethod("DELETE");
			conn.setRequestProperty("Accept-Charset", "utf-8");
			conn.setRequestProperty("contentType", "utf-8");
			conn.setDoOutput(true);
			conn.setDoInput(true);
			OutputStream out = conn.getOutputStream();
			if (StringUtils.isNotBlank(reqBody)) {
				out.write(reqBody.getBytes());
			}
			int code = conn.getResponseCode();
			logger.info(new Integer(code).toString());
			if (code >= 200 && code < 300) {
				result = IoUtility.readStringFromInputStream(
						conn.getInputStream(), "utf-8");
			} else {
				result = IoUtility.readStringFromInputStream(
						conn.getErrorStream(), "utf-8");
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
	// public static String sendReq(String reqURL) {
	// String result = null;
	// BufferedReader in = null;
	// try {
	// HttpURLConnection conn = (HttpURLConnection) new
	// URL(reqURL).openConnection();
	// conn.setRequestMethod("GET");
	// conn.setRequestProperty("Accept-Charset", "utf-8");
	// conn.setRequestProperty("contentType", "utf-8");
	// conn.setDoOutput(true);
	// conn.setDoInput(true);
	// int code = conn.getResponseCode();
	// if (code >= 200 && code < 300) {
	// return IoUtility.readStringFromInputStream(conn.getInputStream(),
	// "utf-8");
	// }
	// } catch (Exception e) {
	// logger.error("sendReq", e);
	// } finally {
	// if (in != null)
	// try {
	// in.close();
	// } catch (IOException e) {
	// }
	// }
	// // 返回
	// return result;
	// }
}
