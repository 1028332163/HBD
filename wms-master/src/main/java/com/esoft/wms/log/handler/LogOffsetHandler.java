package com.esoft.wms.log.handler;

import java.io.IOException;

import javax.ws.rs.core.Response;

import org.atmosphere.config.service.ManagedService;
import org.atmosphere.config.service.Post;
import org.atmosphere.cpr.AtmosphereResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esoft.wms.log.sset.LogResource;
import com.esoft.wms.utils.JsonIoConvertor;
import com.esoft.wms.utils.StringHelper;
import com.esoft.wms.vo.log.LogInfo;

@ManagedService(path = "/logOffset")
public class LogOffsetHandler {

	private final Logger logger = LoggerFactory
			.getLogger(LogOffsetHandler.class);

	@Post
	public void onMessage(final AtmosphereResource resource) {
		// 读取请求json 数据
		String message = "";
		LogInfo logInfo = null;
		try {
			message = resource.getRequest().getReader().readLine();
			logger.info("logOffset:" + message);

			logInfo = (LogInfo) new JsonIoConvertor().read(message,
					LogInfo.class);
		} catch (IOException e1) {
			logger.error("get reader error", e1);
		} catch (Throwable e) {
			logger.error("get json message error", e);
		}

		try {
			LogResource lr = new LogResource(logInfo.getFileName(),
					logInfo.getLogType());
			Response currentLog = lr.currentLog(logInfo);
			

			if (logger.isDebugEnabled()) {
				// 级别高于debug 则不走打印，省去 String.valueOf(currentLog.getEntity()) 操作
				logger.debug(String.valueOf(currentLog.getEntity()));
			}
//			logger.info(new JsonIoConvertor().write(currentLog));
			resource.getResponse().write(
					StringHelper.decodeHtml(new JsonIoConvertor()
							.write(currentLog)));
		} catch (IOException e1) {
			resource.getResponse().write("noLogFile");
		} catch (Throwable e) {
			logger.error("message to Json error", e);
		}

	}
}
