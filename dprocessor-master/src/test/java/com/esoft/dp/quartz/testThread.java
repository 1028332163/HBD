package com.esoft.dp.quartz;

import java.util.Calendar;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class testThread implements Runnable{
	protected static Logger logger = LoggerFactory.getLogger(testThread.class);
	private SocketClient socketClient;
	public testThread(SocketClient socketClient){
		this.socketClient=socketClient;
	}
	@Override
	public void run() {
		
		try {
			String ip="192.168.88.104";
			int port=10000;
			int operation=(int)(Math.random()*3);
//			int operation=1;
			if(operation==0){
				String argsJson="{\"operation\":\"exeJob\", \"jobDetailName\":\"hiveclusterJobDetail\", \"jobDetailGroup\":\"DEFAULT\"}";			
				logger.info("******socket立即执行时间"+new Date());
//				logger.info(  new String(socketClient.send("127.0.0.1", 10000, argsJson))  );
				logger.info(  new String(socketClient.send(ip, port, argsJson))  );
			}else if(operation==1){
				Calendar calendar=Calendar.getInstance();
				calendar.setTimeInMillis(calendar.getTimeInMillis()+1000*3L);
				int year=calendar.get(Calendar.YEAR);
				int month=calendar.get(Calendar.MONTH)+1;
				int day= calendar.get(Calendar.DAY_OF_MONTH);
				int hour = calendar.get(Calendar.HOUR_OF_DAY);
				int minute = calendar.get(Calendar.MINUTE);
				int second = calendar.get(Calendar.SECOND);
				String date=year+"/"+month+"/"+day+"-"+hour+":"+minute+":"+second;
				String argsJson="{\"operation\":\"setTimer\", \"jobDetailName\":\"hiveclusterJobDetail\", \"jobDetailGroup\":\"DEFAULT\","
						+"\"startTime\":\""+date+"\"}";
				logger.info(argsJson);
				logger.info("******socket定时执行时间"+calendar.getTime());
				logger.info(  new String(socketClient.send(ip, port, argsJson))  );
			}else if(operation==2){
				String argsJson="{\"operation\":\"modifyCron\", \"triggerName\":\"hiveCronTriggerBean\", \"triggerGroup\":\"DEFAULT\", \"cron\":\"0 0/4 1-22 * * ?\"}";					
					
				logger.info(  new String(socketClient.send(ip, port, argsJson))  );
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		
	}

}
