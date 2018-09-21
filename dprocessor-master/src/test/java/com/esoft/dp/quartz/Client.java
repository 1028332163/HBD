package com.esoft.dp.quartz;



import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class Client {
	protected static Logger logger = LoggerFactory.getLogger(Client.class);
	public static void main(String[] args)throws Exception{
//		String argsJson="{\"operation\":\"exeJob\", \"jobDetailName\":\"hiveclusterJobDetail\", \"jobDetailGroup\":\"DEFAULT\"}";
		SocketClient socketClient=new SocketClient();
//		String host="127.0.0.1";
//		int port=10000;
//		socketClient.send(host, port, argsJson);
		for(int i=0;i<400;i++){
			new Thread(new testThread(socketClient)).start();
		}
	}
}
