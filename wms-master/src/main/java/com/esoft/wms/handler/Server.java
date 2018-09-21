package com.esoft.wms.handler;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.atmosphere.config.service.Disconnect;
import org.atmosphere.config.service.Get;
import org.atmosphere.config.service.ManagedService;
import org.atmosphere.config.service.Message;
import org.atmosphere.config.service.Post;
import org.atmosphere.config.service.Ready;
import org.atmosphere.cpr.AtmosphereResource;
import org.atmosphere.cpr.AtmosphereResourceEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import com.esoft.wms.WMSMain;
import com.esoft.wms.dao.DaoUtility;
import com.esoft.wms.entity.Job;
import com.esoft.wms.utils.ConstanceInfo;
import com.esoft.wms.utils.JsonIoConvertor;
import com.esoft.wms.utils.SqlTemplate;
import com.esoft.wms.entity.ReceiveVO;

@ManagedService(path = "/handler")
public class Server {
	private final Logger logger = LoggerFactory.getLogger(Server.class);

	private DaoUtility daoUtility;

	private TaskInsHandler taskInsHandler;

	private Map<String, AtmosphereResource> resources;

	public Server() {
		logger.info("construct was invoked");
		ApplicationContext context = WMSMain.context;
		this.taskInsHandler = (TaskInsHandler) context
				.getBean("taskInsHandler");
		this.daoUtility = (DaoUtility) context.getBean("daoUtility");
		this.resources = WMSMain.linkResources;
	}

	@Ready
	public void onReady(AtmosphereResource resource) {
		
		// 将该resource绑定到广播
		logger.info("onReady " + resource.uuid());
		// 用户id，用于校验资源是否在调优范围
		String projectId = resource.getRequest().getParameter("projectId");
		String linkUserId = resource.getRequest().getParameter("userId");
		logger.info("connect:projectId:" + projectId + ",linkUserId:"
				+ linkUserId);
		if (resources.containsKey(projectId)) {// 此工程已经连接，告知前台
			logger.warn(resource.uuid() + " want to link when " + projectId
					+ " was oprating by " + this.resources.get(projectId));
			resource.getResponse().write("project is oprating");
		} else {
			String linkUuid = resource.uuid();
			resource.session().setAttribute("projectId", projectId);
			// 重置数据库中某个project中的现在操作的用户和resource的uuid
			this.daoUtility.updateWithSql(String.format(
					SqlTemplate.UPDATE_PROJECT_LINK, linkUserId, linkUuid,
					projectId));
			this.resources.put(projectId, resource);
		}

	}

	@Message
	public void onMessage(AtmosphereResource resource) {
		logger.info("postMethod receive a messagec from " + resource.uuid());
	}

	@Get
	public void onGet(AtmosphereResource r) {
		logger.info("onGet:" + r.uuid());
	}

	@Post
	public void onPost(AtmosphereResource resource) {
		logger.info("postMethod receive a messagec from " + resource.uuid());
		// 读取request
		StringBuilder message = new StringBuilder("");
		try {
			BufferedReader bf = resource.getRequest().getReader();
			String line = bf.readLine();
			while (line != null) {
				message.append(line);
				line = bf.readLine();
			}
		} catch (IOException e1) {
			logger.error("read message from recource fail ", message);
		}
		logger.info(message.toString());
		JsonIoConvertor convertor = new JsonIoConvertor();
		// 生成job的序列号
		String serialId = UUID.randomUUID().toString().replace("-", "");
		try {
			ReceiveVO receiveInfo = (ReceiveVO) convertor.read(
					message.toString(), ReceiveVO.class);
			List<Job> exeJobs = receiveInfo.getExeJobs();
			List<Job> waitJobs = receiveInfo.getWaitJobs();
			int jobNum = 0;// 有多少个job要被执行
			if (exeJobs != null) {
				jobNum += exeJobs.size();
			}
			if (waitJobs != null) {
				jobNum += waitJobs.size();
			}
			String projectId = (String) resource.session().getAttribute(
					"projectId");
			// 修改project的剩余未被执行的任务数，之后会根据这个变量判断project是否执行完毕
			this.daoUtility.updateWithSql(String.format(
					SqlTemplate.INIT_PROJECT, jobNum, projectId));
			for (Job job : exeJobs) {// 立即执行的job
				job.setSerialId(serialId);
				this.daoUtility.saveJob(job);
				this.taskInsHandler.handleTaskIns(job.getTaskInsJson());
			}
			for (Job job : waitJobs) {// 等待执行的job
				job.setSerialId(serialId);
				this.daoUtility.saveJob(job);
			}
		} catch (Throwable e) {
			logger.info("onPost error:", e);
		}
	}

	@Disconnect
	public void onDisconnect(AtmosphereResourceEvent event) {
		AtmosphereResource resource = event.getResource();
		String uuid = resource.uuid();
		String projectId = (String) resource.session()
				.getAttribute("projectId");
		this.resources.remove(projectId);
		// 将project的状态置为可操作的状态
		// this.daoUtility.updateWithSql(String.format(SqlTemplate.UPDATE_PROJECT_OP,0,projectId));
		logger.info("loose resource to " + projectId);
		if (event.isCancelled()) {
			logger.info("Browser {} unexpectedly disconnected", uuid);
		} else if (event.isClosedByClient()) {
			logger.info("Browser {} closed the connection", uuid);
		}
	}

}
