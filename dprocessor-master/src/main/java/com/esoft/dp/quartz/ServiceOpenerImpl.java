package com.esoft.dp.quartz;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esoft.dp.convert.IoConvertor;
import com.esoft.dp.convert.JsonIoConvertor;
import com.esoft.dp.utils.enums.RetError;
import com.esoft.dp.vo.SchedulerModifyVO;

/**
 * 
 * @author Lzw
 *
 */
public class ServiceOpenerImpl implements ServiceOpener {
	
	protected static Logger logger = LoggerFactory.getLogger(ServiceOpenerImpl.class);
	
	private SchedulerModifier schedulerModifier;
	
	private SparkCmdExecutor sparkCmdExecutor;
	
	//处理线程数量
	private int threadNum;
	
	private int port;
	
	private String ip;
	
	private int backLog;
	
	@Override
//  开启远程修改scheduler的服务，客户端需要通过socket的Stream传入构造参数VO（SchedulerModifyVO）使用的json串，
//	处理会返回结果对应：
//	true：修改scheduler执行成功
//	false：修改scheduler执行失败
//	无返回结果：服务器端输出流获得失败
//  获取参数失败：服务器不能从连接中成功的获得输入流，或者无法从输入流中读出参数信息
	public void openService() {
		
		ServerSocket serverSocket = null;
		ExecutorService fixedThreadPool = Executors.newFixedThreadPool(threadNum);  
		//创建服务器端口
		try {
			serverSocket = new ServerSocket(port, backLog, InetAddress.getByName(ip));
			while(true) {
			//与客户端进行连接
			final Socket clientSocket = serverSocket.accept();
			logger.info("connect：{}", clientSocket);		
			fixedThreadPool.execute(
			new Runnable() {  
					 public void run() {   
					       //从socket中传过来的输入流中构造参数数组List<String> clientArgs
						PrintWriter out = null;
						BufferedReader in = null;
						IoConvertor convertor=new JsonIoConvertor();
						try {
							out = new PrintWriter(clientSocket.getOutputStream());
							in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
							String argsJson = in.readLine();						
							logger.info("read params:{}", argsJson);
							if(argsJson.startsWith("sparkCmd:")){//sparkCmd:cmd 形式的json为执行spark命令
								logger.info("sparkCmd was executed by socket");
								String cmd = argsJson.substring(argsJson.indexOf(":")+1, argsJson.length());
								String result = sparkCmdExecutor.exeSparkCmd(cmd);
								out.println(result);
								out.flush();
							}else{//对quartz进行操作
								logger.info("scheduler was modified by socket");
								try {
									SchedulerModifyVO resultVo = schedulerModifier.modifyScheduler(argsJson);
									out.println(convertor.write(resultVo));//抛出 异常
									out.flush();
								} catch (Throwable e) {									
									logger.error(e.toString());
									//回写获取参数失败
									SchedulerModifyVO resultVo = new SchedulerModifyVO();
									resultVo.setIsOK("false");
									resultVo.setFailDesc(RetError.READ_LINE_ERROR.getMsg());
									try {
										out.println(convertor.write(resultVo));
									} catch (Throwable e1) {
										
										logger.error(e1.toString());
									}
									logger.error("getparams error", e);
								}								
							}

						} catch (IOException e) {
							logger.error("", e);
						} finally{
							try {
								in.close();
								out.close();
							} catch (IOException e) {
							}
						}							
					 } //run方法 
			     }//runnable类
				); //cachedThreadPool.execute参数括号
			}//while死循环
			} catch (Exception e1) {
			// TODO Auto-generated catch block
				logger.error("openModifyService error", e1);
			} finally{
				try {
					serverSocket.close();
				} catch (IOException e) {
				}
			}
		////////////////////////////////////////////////////////////////////////////////////////
	}
	
	public void setSchedulerModifier(SchedulerModifier schedulerModifier){
		this.schedulerModifier = schedulerModifier;
	}
	public SchedulerModifier getScheduler( ){
		return this.schedulerModifier;
	}
	public void setThreadNum(int threadNum){
		this.threadNum=threadNum;
	}
	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public int getBackLog() {
		return backLog;
	}

	public void setBackLog(int backLog) {
		this.backLog = backLog;
	}

	public int getThreadNum(){
		return this.threadNum;
	}
	public void setPort(int port){
		this.port=port;
	}	
	public int getPort(){
		return this.port;
	}

	public SparkCmdExecutor getSparkCmdExecutor() {
		return sparkCmdExecutor;
	}

	public void setSparkCmdExecutor(SparkCmdExecutor sparkCmdExecutor) {
		this.sparkCmdExecutor = sparkCmdExecutor;
	}

	public SchedulerModifier getSchedulerModifier() {
		return schedulerModifier;
	}

}
