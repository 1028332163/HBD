package com.esoft.wms.utils;

import java.lang.reflect.Method;
import java.util.Comparator;

import org.codehaus.plexus.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esoft.wms.entity.TaskInfo;
import com.esoft.wms.entity.TaskInstance;

public class CommonUtils {

	@SuppressWarnings("unused")
	private static Logger logger = LoggerFactory.getLogger(CommonUtils.class);

	private static final String COMMAND_SEPERATOR = " ";

	private static final String CONF_SEPERATOR = "--conf";

	private static final String PARM_SEPERATOR = "--";

	private static final String MASTER_SEPERATOR = "--master";

	private static final String YARN_CLIENT = "yarn-client";

	private static final String CLASS_SEPERATOR = "--class";

	private static final String QUEUE = "--queue";

	private static final String DEFAULT_QUEUE = "default";

	private static final String DRIVER_CLASS = "--driver-class-path";

	private static final String hadoopFS = Config.props
			.getProperty("commandUtils.hadoopFS");
	// private static final String hadoopFS = "hdfs://192.168.88.10:8020/";
	public static final String EXE_MEM = "spark.executor.memory=";

	public static final String DRIVER_MEM = "spark.driver.memory=";

	public static final String exe_num = "num-executors ";

	private CommonUtils() {
	}

	/**
	 * taosh 拼接Spark submit 命令
	 * 
	 * @return
	 */
	public static String initSparkSubmitCmd(TaskInfo info,
			TaskInstance taskInstance) {
		// 切割此算法的所有参数
		// String[] strs = info.getTaskParam().split(";");

		StringBuilder sb = new StringBuilder(info.getBashPath());
		// --conf spark.executor.memory=1g
		sb.append(COMMAND_SEPERATOR);
		sb.append(CONF_SEPERATOR);
		sb.append(COMMAND_SEPERATOR);
		if (taskInstance.getExecutorMem() != null
				&& taskInstance.getExecutorMem() != "") {
			sb.append(EXE_MEM);
			sb.append(taskInstance.getExecutorMem());
			sb.append("g");
		} else {
			sb.append(info.getExecutorMem());
		}
		// --conf spark.driver.memory=1g
		sb.append(COMMAND_SEPERATOR);
		sb.append(CONF_SEPERATOR);
		sb.append(COMMAND_SEPERATOR);
		if (taskInstance.getDriverMem() != null
				&& taskInstance.getDriverMem() != "") {
			sb.append(DRIVER_MEM);
			sb.append(taskInstance.getDriverMem());
			sb.append("g");
		} else {
			sb.append(info.getDriverMem());
		}

		// --num-executors 1
		sb.append(COMMAND_SEPERATOR);
		sb.append(PARM_SEPERATOR);
		if (taskInstance.getNumExecutors() != null
				&& taskInstance.getNumExecutors() != "") {
			sb.append(exe_num);
			sb.append(taskInstance.getNumExecutors());
		} else {
			sb.append(info.getNumExecutors());
		}

		// --master yarn-client
		sb.append(COMMAND_SEPERATOR);
		sb.append(MASTER_SEPERATOR);
		sb.append(COMMAND_SEPERATOR);
		sb.append(YARN_CLIENT);

		// --queue
		sb.append(COMMAND_SEPERATOR);
		sb.append(QUEUE);
		sb.append(COMMAND_SEPERATOR);
		sb.append(DEFAULT_QUEUE);

		// --driverClass
		sb.append(COMMAND_SEPERATOR);
		sb.append(DRIVER_CLASS);
		sb.append(COMMAND_SEPERATOR);
		sb.append(info.getDriverClass());

		// --class
		sb.append(COMMAND_SEPERATOR);
		sb.append(CLASS_SEPERATOR);
		sb.append(COMMAND_SEPERATOR);
		sb.append(info.getTaskClass());
		sb.append(COMMAND_SEPERATOR);
		sb.append(info.getTaskPackage());

		// basePath
		sb.append(COMMAND_SEPERATOR);
		sb.append(hadoopFS);
		// 前台传过来的算法参数
		sb.append(COMMAND_SEPERATOR);
		sb.append(appendQuote(taskInstance.getAlArg()));
		// 最后一个参数是ID
		sb.append(COMMAND_SEPERATOR);
		sb.append(taskInstance.getId());
		String result = sb.toString();

		return result;
	}

	public static String appendQuote(String str) {
		return "\"" + str.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
	}

	public static Comparator<Method> NAME_ASCENDING = new Comparator<Method>() {
		@Override
		public int compare(Method m1, Method m2) {
			final int comparison = m1.getName().compareTo(m2.getName());
			if (comparison != 0) {
				return comparison;
			}
			return m1.toString().compareTo(m2.toString());
		}
	};

	/**
	 * 拼接在某属性的 set方法
	 * 
	 * @param fieldName
	 * @return String
	 */
	public static String parSetName(String fieldName) {
		if (StringUtils.isBlank(fieldName)) {
			return null;
		} else {
			// 属性字段全部为大写字母TASK_NAME
			fieldName = fieldName.toLowerCase();
		}
		int startIndex = 0;
		if (fieldName.indexOf("_") != -1) {
			return "get"
					+ fieldName.substring(startIndex, startIndex + 1)
							.toUpperCase()
					+ fieldName.substring(startIndex + 1,
							fieldName.indexOf("_"))
					+ fieldName.substring(fieldName.indexOf("_") + 1,
							fieldName.indexOf("_") + 2).toUpperCase()
					+ fieldName.substring(fieldName.indexOf("_") + 2);
		}
		return "get"
				+ fieldName.substring(startIndex, startIndex + 1).toUpperCase()
				+ fieldName.substring(startIndex + 1);
	}

	/**
	 * 替换掉相应特殊字符
	 * 
	 * @param regex
	 * @param target
	 * @return
	 */
	public static String filterSpeChars(String regex, String target) {
		return target.replaceAll(regex, "");
	}

}
