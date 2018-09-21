package com.esoft.wms.utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;

import org.apache.commons.io.IOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Socket 客户端， 设置最大连接数量
 * 
 * @author taoshi
 *
 */
public class SocketClient {
	
	private static Logger logger = LoggerFactory
			.getLogger(SocketClient.class);
	
	private String ip;
	
	private String port;
	// 当前连接数量
	private Integer	current	= 0;

	// 同步符号
	private Integer	signal	= 1;

	// 建立连接重试次数
	private int		retry	= 3;

	public int getRetry() {
		return retry;
	}

	public void setRetry(int retry) {
		this.retry = retry;
	}

	// 建立连接重试超时时间
	private long	retryTimeout	= 5000;

	public long getRetryTimeout() {
		return retryTimeout;
	}

	public void setRetryTimeout(long retryTimeout) {
		this.retryTimeout = retryTimeout;
	}

	// 最大连接数量
	private int	maxCount	= 100;

	public int getMaxCount() {
		return maxCount;
	}

	public void setMaxCount(int maxCount) {
		this.maxCount = maxCount;
	}

	// 关闭连接等待时间
	private int	soLingerTime	= 5;

	public int getSoLingerTime() {
		return soLingerTime;
	}

	public void setSoLingerTime(int soLingerTime) {
		this.soLingerTime = soLingerTime;
	}

	/**
	 * 通过Socket发送并返回数据
	 * 
	 * @param host
	 *            主机名或地址
	 * @param port
	 *            端口
	 * @param data
	 *            请求数据
	 * @return 返回响应数据
	 * @throws IOException
	 *             输入输出异常
	 * @throws UnknownHostException
	 *             主机名未知
	 * @throws InterruptedException
	 *             建立连接超时
	 */
	public String send(String data)
			throws UnknownHostException, IOException, InterruptedException {
		// 进入
		for (int i = 0; i < retry; i++) {
			synchronized (signal) {
				if (current < maxCount) {
					current++;
					break;
				} else {
					try {
						signal.wait(retryTimeout);
					} catch (InterruptedException e) {
						if (i == retry - 1) throw e;
					}
				}
			}
		}

		Socket client = null;
		try {
			client = new Socket();
			// 设置关闭时socket的TIME_WAIT时间
			client.setSoLinger(true, soLingerTime);
			// 建立连接
			client.connect(new InetSocketAddress(ip, Integer.parseInt(port)));
			// 输出数据
			PrintWriter out = new PrintWriter(client.getOutputStream(),true);
			//将数据写入到服务器
			out.println(data);
			
			out.flush();
			// 输入数据
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			
			IOUtils.copy(client.getInputStream(), bos);
			
			String result = new String(bos.toByteArray());
			
			logger.debug("call result:{}", result);
			
			return result;
		} finally {
			// 关闭
			if (client != null) client.close();
			// 释放
			synchronized (signal) {
				current--;
				signal.notifyAll();
			}
		}
	}
	
	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public String getPort() {
		return port;
	}

	public void setPort(String port) {
		this.port = port;
	}
}
