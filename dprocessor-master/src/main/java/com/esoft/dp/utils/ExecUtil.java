package com.esoft.dp.utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.PumpStreamHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExecUtil {
	private static Logger logger = LoggerFactory
			.getLogger(ExecUtil.class);
	
	/**
	 * 执行命令或者shell并获取返回内容(例如 /usr/local/Test.sh)
	 * @param com
	 * @return String
	 */
	public static String exec(String com){
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();  
        ByteArrayOutputStream errorStream = new ByteArrayOutputStream();  
        PumpStreamHandler streamHandler = new PumpStreamHandler(outputStream,errorStream); 
        
        String out = null;  
        String error = null;  
        
		CommandLine commandline = CommandLine.parse("bash");//bash 相当于sh
		commandline.addArgument("-c", false);//-c command
		commandline.addArgument(com, false);//com 要执行的命令或者脚本
		
		final DefaultExecutor executor = new DefaultExecutor();
		//设置处理标注输出和错误输出的Handler
		executor.setStreamHandler(streamHandler);
		try {
			int exitValue = executor.execute(commandline);//执行
			logger.info("ExitValue:{}",exitValue);
			out = outputStream.toString("utf-8");
			error = errorStream.toString("utf-8");
		} catch (IOException e) {
			logger.error("run error:{}",e);
		} finally {
			try {
				outputStream.close();
				errorStream.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return out+error;
	}
	
	/**
	 * 执行命令并获取返回内容(例如 java -jar Test.jar)
	 * @param com
	 * @return String
	 */
	public static String execJava(String com){
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();  
        ByteArrayOutputStream errorStream = new ByteArrayOutputStream();  
        PumpStreamHandler streamHandler = new PumpStreamHandler(outputStream,errorStream);  
        
        String out = null;  
        String error = null;
        
		CommandLine commandline = CommandLine.parse(com);
		final DefaultExecutor executor = new DefaultExecutor();
		//设置处理标注输出和错误输出的Handler
		executor.setStreamHandler(streamHandler);
		try {
			int exitValue = executor.execute(commandline);//执行
			logger.info("ExitValue:{}",exitValue);
			out = outputStream.toString("utf-8");
			error = errorStream.toString("utf-8");
		} catch (IOException e) {
			logger.error("run error:{}",e);
		} finally {
			try {
				outputStream.close();
				errorStream.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return out+error;
	}
	
}
