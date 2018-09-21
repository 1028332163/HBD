package com.esoft.wms.log.handler;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.Response;

import org.apache.commons.io.input.Tailer;
import org.apache.commons.io.input.TailerListener;
import org.apache.commons.io.input.TailerListenerAdapter;
import org.atmosphere.config.service.Disconnect;
import org.atmosphere.config.service.Get;
import org.atmosphere.config.service.ManagedService;
import org.atmosphere.cpr.AtmosphereRequest;
import org.atmosphere.cpr.AtmosphereResource;
import org.atmosphere.cpr.AtmosphereResourceEvent;
import org.atmosphere.cpr.AtmosphereResourceEventListenerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esoft.wms.log.sset.LogResource;
import com.esoft.wms.utils.JsonIoConvertor;
import com.esoft.wms.utils.StringHelper;

/**
 * 
 * @author taos 监控日志日志变化，并推送到前端
 * 
 */
@ManagedService(path = "/log")
public class LogHandler {

	private final Logger logger = LoggerFactory.getLogger(LogHandler.class);

	Tailer tailer = null;

	/**
	 * 开启websoket 连接
	 * 
	 * @param r
	 */
	@Get
	public void onOpen(final AtmosphereResource r) {
		r.addEventListener(new AtmosphereResourceEventListenerAdapter() {
			@Override
			public void onSuspend(AtmosphereResourceEvent event) {
				try {
					AtmosphereRequest request = r.getRequest();

					final String logFile = request.getParameter("fileName");// 日志名称
					final String logType = request.getParameter("logType");// 日志类别
					final LogResource lr = new LogResource(logFile, logType);

					final String jobid = request.getParameter("jobid");
					logger.info("jobid:" + jobid);

					TailerListener listener = new TailerListenerAdapter() {
						// 刷新时如果出现
						Map<String, String> errorLogCache = new HashMap<String, String>();

						@Override
						public void handle(String line) {
							// 处理新生成的日志文件，使生成的日志按照固定的格式输出
							Map<String, String> newLog = lr.newOrderShow(line,
									jobid, null);
							if (newLog != null) {
								if (newLog.containsKey("exception")) {
									// 将不能解析的数据缓存
									if (errorLogCache.isEmpty()) {// 直接把新的exception放入
										errorLogCache.put("exception",
												newLog.get("exception"));
									} else {// 把新的exception放到cache的后面
										errorLogCache.put(
												"exception",
												errorLogCache.get("exception")
														+ "\n"
														+ newLog.get("exception"));
									}
								} else {
									// 将解析的日志和
									List<Map<String, String>> retLog = new ArrayList<Map<String, String>>();
									if (!errorLogCache.isEmpty()) {// 将错误日志跟新日志一起返回，并清空缓存
										retLog.add(errorLogCache);
									}
									retLog.add(newLog);
									try {
										// 转换成json形式，传递到页面
										Response currentLog = Response
												.ok()
												.type("application/json")
												.entity(retLog)
												.header("AUTOMATIC_PUSH_INFOMATION",
														"LogHandler").build();
										String returnJson = StringHelper
												.decodeHtml(new JsonIoConvertor()
														.write(currentLog));
										logger.info("new line:" + returnJson);
										r.getResponse().write(returnJson);
										errorLogCache.clear();
									} catch (Throwable e) {
										logger.error("currentLog to json:", e);
									}
								}
							}
						}

						// log when file not found
						@Override
						public void fileNotFound() {
							throw new IllegalArgumentException("Logfile '"
									+ logFile + "' not found");
						}

						// log when handle exception
						@Override
						public void handle(Exception ex) {
							logger.warn(
									"Error while trying to read log file '{}': {}",
									logFile, ex.toString(), ex);
						}
					};

					// TODO send -20K of logFile to session, separator line,
					// then tailer
					File f = new File(logFile);
					tailer = new Tailer(f, listener, 100, true, true);
					Thread thread = new Thread(tailer, "LogHandler-tailLog");
					thread.setDaemon(true);
					thread.start();
					logger.info("User {} connected.", r.uuid());
				} catch (Throwable e) {
					logger.error("get json message error", e);
				}
			}

			@Override
			public void onDisconnect(AtmosphereResourceEvent event) {
				if (event.isCancelled()) {
					logger.info("User {} unexpectedly disconnected", r.uuid());
				} else if (event.isClosedByClient()) {
					logger.info("User {} closed the connection", r.uuid());
				}
				if (null != tailer) {
					logger.info("tailer stop!");
					tailer.stop();
				}
			}
		});
	}

	/**
	 * 断开websoket 连接
	 * 
	 * @param event
	 */
	@Disconnect
	public void onDisconnect(AtmosphereResourceEvent event) {
		if (null != tailer) {
			logger.info("tailer stop!");
			tailer.stop();
		}
	}
}
