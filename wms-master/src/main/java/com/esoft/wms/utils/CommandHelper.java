package com.esoft.wms.utils;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.PumpStreamHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommandHelper {
	
	protected static Logger logger = LoggerFactory.getLogger(CommandHelper.class);
	
	private static final int commandTimeOut = 60000000;
	
	private CommandHelper(){}
	/**
	 * 执行sh 命令
	 * @param command:命令内容
	 * @param key:命令标识符，日志查看方便
	 */
	public static void execCommand(String command, String key){
		
		CommandLine commandline = CommandLine.parse("bash");
	    
		commandline.addArgument("-c", false);
		   
		logger.debug("id =>{},spark content ==>{}",key,command);
		  
		commandline.addArgument(command,false);
		  
		DefaultExecutor executor = new DefaultExecutor();
	  
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
	    
		ByteArrayOutputStream errorStream = new ByteArrayOutputStream();  
	    
		executor.setStreamHandler(new PumpStreamHandler(outputStream,errorStream));
	
		executor.setWatchdog(new ExecuteWatchdog(commandTimeOut));
		    
		  try {
		      int exitValue = executor.execute(commandline);
		      logger.info("===>"+exitValue);
		      String out = outputStream.toString("gbk");  
	          String error = errorStream.toString("gbk"); 
	          logger.info("return content:"+ out);
	          logger.info("return error content:"+ error);
		    } catch (ExecuteException e) {
		      logger.error("Can not run command", e);
		    } catch (IOException e) {
		      logger.error("Can not run ", e);
		    }
	}

}
