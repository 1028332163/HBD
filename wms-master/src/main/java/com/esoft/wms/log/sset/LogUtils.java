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

import com.esoft.wms.log.parser.Grok;
import com.esoft.wms.log.parser.GrokDictionary;
import com.esoft.wms.log.parser.Log4jHelper;
import com.esoft.wms.utils.ConstanceInfo;
import com.google.common.annotations.VisibleForTesting;

public class LogUtils {
	public static final String LOG4J_FILE_ATTR = "log4j.filename";
	public static final String LOG4J_APPENDER_STREAMSETS_FILE_PROPERTY = "log4j.appender.streamsets.File";
	public static final String LOG4J_APPENDER_STREAMSETS_LAYOUT_CONVERSION_PATTERN = "log4j.appender.streamsets.layout.ConversionPattern";
	public static final String LOG4J_APPENDER_STDERR_LAYOUT_CONVERSION_PATTERN = "log4j.appender.stderr.layout.ConversionPattern";
	public static final String LOG4J_GROK_ATTR = "log4j.grok";
	//jobServer 日志格式
	public static final String LOG4J_JOBSERVER_PATTERN = "[%d] %-5p [%X{codePos}] [%X{akkaSource}] - %m%n";
	
	//普通spark 任务格式
	public static final String LOG4J_SIMPLE_PATTERN = "%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n";

	private LogUtils() {
	}
	/**
	 * @author taos
	 * 根据日志类型，判断jobserver 的日志或者是普通日志
	 * @param logType
	 * @return
	 * @throws Exception
	 */
	public static Grok getLogGrok(String logType) throws Exception {
		//这里配置上 log4j.pro...等文件中的   log4j.appender.streamsets.layout.ConversionPattern 属性
		String grokPattern = null;
		if(logType.equals(ConstanceInfo.JOBSERVER)){
			grokPattern = Log4jHelper.translateLog4jLayoutToGrok(LOG4J_JOBSERVER_PATTERN);
    	} else {
    		//定时任务触发的spark job 日志
    		grokPattern = Log4jHelper
			.translateLog4jLayoutToGrok(LOG4J_SIMPLE_PATTERN);
    	}
		GrokDictionary grokDictionary = new GrokDictionary();
		grokDictionary.addDictionary(LogUtils.class.getClassLoader()
				.getResourceAsStream("grok-patterns"));
		grokDictionary
				.addDictionary(LogUtils.class.getClassLoader()
						.getResourceAsStream(
								"java-log"));
		grokDictionary.bind();
		Grok logFileGrok = grokDictionary.compileExpression(grokPattern);

		return logFileGrok;
	}

	@VisibleForTesting
	static String resolveValue(String str) {
		while (str.contains("${")) {
			int start = str.indexOf("${");
			int end = str.indexOf("}", start);
			String value = System.getProperty(str.substring(start + 2, end));
			String current = str;
			str = current.substring(0, start) + value
					+ current.substring(end + 1);
		}
		return str;
	}
}
