/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.esoft.wms.log.sset;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.Response;

import org.codehaus.plexus.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esoft.wms.log.parser.Grok;
import com.esoft.wms.vo.log.LogInfo;

public class LogResource {
	public static final String X_SDC_LOG_PREVIOUS_OFFSET_HEADER = "X-SDC-LOG-PREVIOUS-OFFSET";
	public static final String LOGDATA_LEN = "logDataLen";
	public static final String MARK = "MARK";
	public static final String SEVERITY = "SEVERITY";
	private static final String EXCEPTION = "exception";
	private String logFile;
	private final Grok logFileGrok;
	public static final String AUTOMATIC_PUSH = "AUTOMATIC_PUSH_INFOMATION";
	private final Logger logger = LoggerFactory.getLogger(LogResource.class);

	public LogResource(String logFile, String regular) {
		try {
			this.logFile = logFile;
			logFileGrok = LogUtils.getLogGrok(regular);
		} catch (Exception ex) {
			throw new IllegalStateException(
					"Can't load logging infrastructure", ex);
		}
	}

	/**
	 * @param logFileName
	 * @param startOffset
	 *            偏移量
	 * @param extraMessage
	 * @param severity
	 *            日志级别选择
	 * @return
	 * @throws IOException
	 */
	public Response currentLog(LogInfo logInfo) throws IOException {

		String jobid = logInfo.getJobid();
		List<Map<String, String>> logData = new ArrayList<>();
		long offset = Long.parseLong(logInfo.getOffset());// 获取偏移量

		if (logInfo.getFileName() != null) {
			logFile = logInfo.getFileName();
		}
		// 加载指定大小的字符串 到 outputstream
		LogStreamer streamer = new LogStreamer(logFile, offset, 10 * 1024,
				jobid);
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		streamer.stream(outputStream);

		if (logInfo.getExtraMessage() != null) {// 将上一次读到的半条信息拼到outputstream
			outputStream.write(logInfo.getExtraMessage().getBytes(
					StandardCharsets.UTF_8));
		}

		BufferedReader bufferedReader = new BufferedReader(
				new InputStreamReader(new ByteArrayInputStream(
						outputStream.toByteArray()), StandardCharsets.UTF_8));

		this.fetchLogData(bufferedReader, logData, logInfo, logFile);

		offset = streamer.getNewEndingOffset();
		streamer.close();

		if (logData.size() < 50) {
			// 如果取回的数据不够五十条继续向文件的前面找
			while (offset != 0 && logData.size() < 50) {
				streamer = new LogStreamer(logFile, offset, 10 * 1024, jobid);
				outputStream = new ByteArrayOutputStream();
				streamer.stream(outputStream);

				// merge last message if it is part of new messages
				// ?
				// if (!logData.isEmpty()
				// && logData.get(0).get("timestamp") == null
				// && logData.get(0).get(EXCEPTION) != null) {
				// outputStream.write(logData.get(0).get(EXCEPTION)
				// .getBytes(StandardCharsets.UTF_8));
				// logData.remove(0);
				// }
				if (!logData.isEmpty()
						&& logData.get(logData.size() - 1).get(EXCEPTION) != null) {
					outputStream.write(logData.get(logData.size() - 1)
							.get(EXCEPTION).getBytes(StandardCharsets.UTF_8));
					logData.remove(logData.size() - 1);
				}

				bufferedReader = new BufferedReader(new InputStreamReader(
						new ByteArrayInputStream(outputStream.toByteArray())));

				List<Map<String, String>> tempLogData = new ArrayList<>();
				fetchLogData(bufferedReader, tempLogData, logInfo, logFile);

				// Add newly fetched log data to the beginning of the list
				tempLogData.addAll(logData);
				logData = tempLogData;

				offset = streamer.getNewEndingOffset();
				streamer.close();
			}
		}

		String mark = logInfo.getMark();
		mark += 1;
		logger.info("end offset" + offset);
		logger.info("return log of " + logInfo.getJobid() + " size:"
				+ new Integer(logData.size()).toString());
		return Response.ok().type("application/json").entity(logData)
				.header(X_SDC_LOG_PREVIOUS_OFFSET_HEADER, offset)
				.header(LOGDATA_LEN, logData.size()).header(MARK, mark).build();
	}

	/**
	 * @author lzd 功能：推送日志（自动推送）
	 * @param line
	 *            读取新增日志
	 * @param severity
	 *            日志级别选择
	 * @return Response
	 * @throws IOException
	 */
	public Map<String, String> newOrderShow(String line, String jobid,
			String severity) {
		// 日志级别选择all的时候处理，筛选出指定jobid全部的信息
		if (StringUtils.isNotBlank(severity)) {
			if (severity.equals("ALL")) {
				severity = null;
			}
		}

		Map<String, String> namedGroupToValuesMap = logFileGrok
				.extractNamedGroups(line);
		if (namedGroupToValuesMap != null) {
			// 按照日志级别进行过滤
			if (severity != null
					&& !severity.equals(namedGroupToValuesMap.get("severity"))) {
				return namedGroupToValuesMap;
			}
			return null;			
		} else {
			// 此行不是正常的日志格式，不能进行解析
			Map<String, String> errorLogLine = new HashMap<String, String>();
			errorLogLine.put("exception", line);
			return errorLogLine;
		}
	}

	private void fetchLogData(BufferedReader bufferedReader,
			List<Map<String, String>> logData, LogInfo logInfo, String log)
			throws IOException {
		String severity = logInfo.getSeverity();// 日志级别 info warn error ect

		// 日志级别选择all的时候处理，筛选出指定jobid全部的信息
		if (StringUtils.isNotBlank(severity)) {
			if (severity.equals("ALL")) {
				severity = null;
			}
		}
		String thisLine = null;
		boolean lastMessageFiltered = false;
		while ((thisLine = bufferedReader.readLine()) != null) {
			// logger.info(thisLine);
			Map<String, String> namedGroupToValuesMap = logFileGrok
					.extractNamedGroups(thisLine);
			// logger.info(""+(namedGroupToValuesMap == null));
			if (namedGroupToValuesMap != null) {
				// 按照日志级别进行过滤
				if (severity != null
						&& !severity.equals(namedGroupToValuesMap
								.get("severity"))) {
					lastMessageFiltered = true;
					continue;
				}
				lastMessageFiltered = false;
				logData.add(namedGroupToValuesMap);
			} else if (!lastMessageFiltered) {
				if (!logData.isEmpty()) {
					Map<String, String> lastLogData = logData.get(logData
							.size() - 1);

					if (lastLogData.containsKey(EXCEPTION)) {
						lastLogData.put(EXCEPTION, lastLogData.get(EXCEPTION)
								+ "\n" + thisLine);
					} else {
						lastLogData.put(EXCEPTION, thisLine);
					}
				} else {
					// First incomplete line
					Map<String, String> lastLogData = new HashMap<>();
					lastLogData.put("exception", thisLine);
					logData.add(lastLogData);
				}
			}

		}
	}
	// @SuppressWarnings("unused")
	// private File[] getLogFiles() throws IOException {
	// File log = new File(logFile);
	// File logDir = log.getParentFile();
	// final String logName = log.getName();
	// return logDir.listFiles(new FilenameFilter() {
	// @Override
	// public boolean accept(File dir, String name) {
	// return name.startsWith(logName);
	// }
	// });
	// }
	/**
	 * @author lzd 功能：通过 jobid 查看信息
	 * @param currentLog
	 *            当前生成的日志
	 * @param jobid
	 *            日志ID
	 * @return Response
	 * @throws IOException
	 *             异常
	 */
	// public Response findJonidInformation(Response currentLog,LogInfo logInfo)
	// throws IOException {
	// //获取 页面传入jobid
	// String jobid = logInfo.getJobid();
	// Map<String, Object> mapMess =
	// findMessage(currentLog,logInfo,jobid,"jobId");
	// @SuppressWarnings("unchecked")
	// List<Map<String, String>> list = (List<Map<String, String>>)
	// mapMess.get("lis");
	// String off = (String) mapMess.get("ofi");
	// String mark = (String) mapMess.get("mar");
	// //加载日志文件
	// //LogStreamer streamer = new LogStreamer(logFile, -1, 10 * 1024,jobid);
	// return
	// Response.ok().type("application/json").entity(list).header(X_SDC_LOG_PREVIOUS_OFFSET_HEADER,
	// off).header(MARK, mark).build();
	//
	// }
	/**
	 * @author lzd 功能：通过 secerity 查看日志级别
	 * @param currentLog
	 *            当前生成的日志
	 * @param secerity
	 *            日志级别
	 * @return Response
	 * @throws Throwable
	 */
	// public Response findSecerityInformation(Response currentLog,LogInfo
	// logInfo) throws Throwable {
	// //获取 页面传入日志级别
	// String severity = logInfo.getSeverity();
	// //判断日志级别是否为ALL级别
	// if(severity.equals("ALL")){
	// //获取 页面传入jobid
	// String jobid = logInfo.getJobid();
	// Map<String, Object> mapMess =
	// findMessage(currentLog,logInfo,jobid,"jobId");
	// @SuppressWarnings("unchecked")
	// List<Map<String, String>> list = (List<Map<String, String>>)
	// mapMess.get("lis");
	// String off = (String) mapMess.get("ofi");
	// String mark = (String) mapMess.get("mar");
	// //加载日志文件
	// //LogStreamer streamer = new LogStreamer(logFile, -1, 10 * 1024,jobid);
	// return
	// Response.ok().type("application/json").entity(list).header(X_SDC_LOG_PREVIOUS_OFFSET_HEADER,
	// off).header(MARK, mark).build();
	// } else {
	// //判断日志级别
	// /*
	// if(severity.equals("WARN")){
	//
	// }
	// if(severity.equals("INFO")){
	//
	// }
	// if(severity.equals("ERROR")){
	//
	// }
	// */
	// Map<String, Object> mapMess =
	// findMessage(currentLog,logInfo,severity,"severity");
	// @SuppressWarnings("unchecked")
	// List<Map<String, String>> list = (List<Map<String, String>>)
	// mapMess.get("lis");
	// String off = (String) mapMess.get("ofi");
	// String mark = (String) mapMess.get("mar");
	// return
	// Response.ok().type("application/json").entity(list).header(X_SDC_LOG_PREVIOUS_OFFSET_HEADER,
	// off).header(SEVERITY, severity).header(MARK, mark).build();
	// }
	// }
	/**
	 * @author lzd 功能：查找对应信息
	 * @param currentLog
	 *            当前生成的日志
	 * @param findMe
	 *            前台传入的jobid或日志级别
	 * @param level
	 *            在日志文件中查找对应的字段信息
	 * @param logInfo
	 *            日志参数VO
	 * @return Response
	 * @throws IOException
	 *             异常
	 */
	// public Map<String, Object> findMessage(Response currentLog,LogInfo
	// logInfo,String findMe,String level){
	// logger.info("level :{}",level);
	// Map<String, Object> allParameter = new HashMap<String, Object>();
	// List<Map<String, String>> list = new ArrayList<Map<String,String>>();
	// //得到entity中的object对象中的信息（通过规则处理好形成的指定格式信息）
	// List<?> listEntity = (ArrayList<?>)currentLog.getEntity();
	// //取参数传到前台做验证（off 为偏移量，mark参数是区分默认读取的还是点击查看之前日志）
	// String off = currentLog.getHeaderString("X-SDC-LOG-PREVIOUS-OFFSET");
	// String mark = currentLog.getHeaderString("MARK");
	// //定义数组长度
	// int len = listEntity.size();
	// //通过循环取Entity 日志
	// for(int i =0; i < len -1; i++){
	// @SuppressWarnings("unchecked")
	// HashMap<String, String> infoEntity = (HashMap<String, String>)
	// listEntity.get(i);
	// String secerityValue = infoEntity.get(level);
	// //判断前台传入的日志级别和日志中的级别是否相同
	// if(findMe.equals(secerityValue)){
	// infoEntity.put("X-SDC-LOG-PREVIOUS-OFFSET", off);
	// infoEntity.put("MARK", mark);
	// infoEntity.put("message",
	// StringHelper.decodeHtml(infoEntity.get("message")));
	// list.add(infoEntity);
	// }
	// }
	// allParameter.put("lis", list);
	// allParameter.put("mar", mark);
	// allParameter.put("ofi", off);
	// return allParameter;
	// }
}
