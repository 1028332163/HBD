package com.esoft.dp.quartz;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.Calendar;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.PumpStreamHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author liuzw
 * 将失败的任务重新执行，日志打印到out1，返回fail/success;日期
 */
public class SparkCmdExecutor {
	protected static Logger logger = LoggerFactory.getLogger(SparkCmdExecutor.class);
	
	
	private int commandTimeOut;	

	public int getCommandTimeOut() {
		return commandTimeOut;
	}

	public void setCommandTimeOut(int commandTimeOut) {
		this.commandTimeOut = commandTimeOut;
	}
	
	public String exeSparkCmd(String cmd){
		logger.info(cmd);
		FileOutputStream outputStream = null, errorStream = null;// 将spark任务返回的标准输出和错误输出到文件		
		// 根据当前日期构建一个文件夹，存放上述文件
		Calendar calendar = Calendar.getInstance();
		int month = calendar.get(Calendar.MONTH) + 1;// 因为月份在calendar中的存储是比实际小1所以要处理
		String date = "" + calendar.get(Calendar.YEAR) + month
				+ calendar.get(Calendar.DAY_OF_MONTH);
		String dirPath = "out1/"+date;
		File file = new File(dirPath);
		if (!file.exists() || !file.isDirectory()) {
			file.mkdirs();
		}
		// avoid unix error
		CommandLine commandline = CommandLine.parse("bash");
		commandline.addArgument("-c", false);
		//获取任务id
		String taskId = cmd.substring(cmd.lastIndexOf(" ")+1, cmd.length());
		logger.info(taskId);
		//spark任务执行信息的输出路径            例：out/20161112/stdOut55
		String stdOutPath = dirPath+"/stdOut"+ taskId +".txt";
		String errOutPath = dirPath+"/errOut" + taskId +".txt";
		try {	
			outputStream = new FileOutputStream(stdOutPath);
			errorStream = new FileOutputStream(errOutPath);			
			commandline.addArgument(cmd, false);
			final DefaultExecutor executor = new DefaultExecutor();
			executor.setStreamHandler(new PumpStreamHandler(outputStream,
					errorStream));
			executor.setWatchdog(new ExecuteWatchdog(commandTimeOut));
			int exitValue = executor.execute(commandline);
			logger.info("===>" + exitValue);
			return "success;"+date;
		} catch (ExecuteException e) {
			logger.error("Can not run command", e);			
			return "fail;"+date;
		} catch (IOException e) {
			logger.error("Can not run ", e);
			return "fail;"+date;
		} finally {//将task的运行信息放到文件中去，并打印一份错误信息到dp的控制台
			try {
				outputStream.flush();
				logger.info("标准输出已被写进文件" + "stdOut" + taskId);
				errorStream.flush();
				logger.info("错误输出已被写进文件" + "errOut" + taskId);
				outputStream.close();
				errorStream.close();
				///为了调试方便将错误信息在控制台打印一份
				BufferedReader br = new BufferedReader(new FileReader(errOutPath));
				String line = br.readLine();
				while(line!=null){///打印一份错误信息到控制台
					System.out.println(line);
					line = br.readLine();
				}
				br.close();
				br = new BufferedReader(new FileReader(stdOutPath));
				line = br.readLine();
				while(line!=null){///打印一份错误信息到控制台
					System.out.println(line);
					line = br.readLine();
				}				
				br.close();
			} catch (IOException e) {
			}
			
		}
	}
}
