package com.esoft.dp.mgr.impl;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Calendar;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.input.Tailer;
import org.apache.commons.io.input.TailerListenerAdapter;
import org.codehaus.plexus.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esoft.dp.entity.Ancestor;
import com.esoft.dp.entity.Task;

public class SparkSubmitCommandMgrImpl extends CommandExecMgrImpl{
	
	private String errLogDir;
	
	private static Logger logger = LoggerFactory
			.getLogger(SparkSubmitCommandMgrImpl.class);

	Tailer tailer = null;
	
	public void generateCommand(Ancestor t) {
		FileOutputStream outputStream = null, errorStream = null;// 将spark任务返回的标准输出和错误输出到文件		
		// 根据当前日期构建一个文件夹，存放上述文件
		Calendar calendar = Calendar.getInstance();//获取当前时间
		int month = calendar.get(Calendar.MONTH) + 1;// 因为月份在calendar中的存储是比实际小1所以要处理
		String date = "" + calendar.get(Calendar.YEAR) + month
				+ calendar.get(Calendar.DAY_OF_MONTH);
		String dirPath = errLogDir + date;
		File file = new File(dirPath);
		if (!file.exists() || !file.isDirectory()) {
			file.mkdirs();
		}
		// 获取任务实例对象
		Task sparkTask = (Task) t;
		// avoid unix error
		CommandLine commandline = CommandLine.parse("bash");
		commandline.addArgument("-c", false);
		// 获取spark 执行命令
		String command = sparkTask.getSparkCmdContent();
		logger.debug("id =>{},spark content ==>{}", sparkTask.getId(), command);
		//spark任务执行信息的输出路径            例：out/20161112/stdOut55
		String stdOutPath = dirPath + "/stdOut"+ sparkTask.getId() + ".txt";
		String errOutPath = dirPath + "/errOut" + sparkTask.getId() + ".txt";
		try {	
			outputStream = new FileOutputStream(stdOutPath);
			errorStream = new FileOutputStream(errOutPath);
			String sql = "update TB_DAE_TARSK_INSTANCE  set LOGPATH = '" 
					+ date
				    + "' where id =" +sparkTask.getId();
			super.daoUtility.deleteWithSql(sql);//将日志的路径放到数据库中的TaskInstance中
			
			commandline.addArgument(command, false);
			final DefaultExecutor executor = new DefaultExecutor();
			executor.setStreamHandler(new PumpStreamHandler(outputStream,
					errorStream));
			executor.setWatchdog(new ExecuteWatchdog(commandTimeOut));
			
			tailSpark(sparkTask, errOutPath);
			
			logger.info("This spark comm:{}",commandline.toString());
			int exitValue = executor.execute(commandline);
			logger.info("===>" + exitValue);
			
			// String out = outputStream.toString("gbk");
			// logger.info("return content:"+ out);
		} catch (ExecuteException e) {
			logger.error("Can not run command", e);			
			after(sparkTask);
		} catch (IOException e) {
			logger.error("Can not run ", e);

		} catch (Exception e) {
			logger.error("Can not run tailSpark", e);
		} finally {//将task的运行信息放到文件中去，并打印一份错误信息到dp的控制台
			try {
				outputStream.flush();
				logger.info("stdout has been written!" + "stdOut" + sparkTask.getId());
				errorStream.flush();
				logger.info("stdErr has been written!" + "errOut" + sparkTask.getId());
				outputStream.close();
				errorStream.close();
			} catch (IOException e) {
			}
		}
	}
	
	public void tailSpark(Task t,String filePath) throws Exception{ 
		
		logger.info("Begin tailSpark");
		
		final Task sparkTask = t;
        File file = new File(filePath);  
        FileUtils.touch(file);  
  
        tailer = new Tailer(file,new TailerListenerAdapter(){  
  
            @Override  
            public void fileNotFound() {  //文件没有找到  
                System.out.println("文件没有找到");  
                super.fileNotFound();  
            }  
  
            @Override  
            public void fileRotated() {  //文件被外部的输入流改变  
                System.out.println("文件rotated");  
                super.fileRotated();  
            }  
  
            @Override  
            public void handle(String line) { //增加的文件的内容  
                System.out.println("文件line:"+line); 
                if (line.contains("tracking URL")) {
                	String[] str = line.split("/");
                	String applicationId = str[str.length-1].toString();
                	update(sparkTask,applicationId);
                	tailer.stop();
                	logger.info("Thread exit");
                }
                super.handle(line);  
            }  
  
            @Override  
            public void handle(Exception ex) {  
                ex.printStackTrace();  
                super.handle(ex);  
            }  
        },0,true);  
        new Thread(tailer).start();  
    } 
	
	public void update(Object t,String applicationId) {
		if(StringUtils.isNotBlank(applicationId)){
			applicationId = applicationId.replaceAll(":", " ");
		}
		StringBuilder sb = new StringBuilder("update");
		Integer targetID = null;
		targetID = ((Task) t).getId();
		sb.append(" TB_DAE_TARSK_INSTANCE t set t.APPLICATION_ID = '");
		sb.append(applicationId);
		sb.append("'");
		sb.append(" where t.id = ");
		sb.append(targetID);
		logger.info("exec after sql:,{}",sb.toString());
		//更新 记录状态
		super.daoUtility.deleteWithSql(sb.toString());
	}
	public void setErrLogDir(String errLogDir) {
		this.errLogDir = errLogDir;
	}

}
