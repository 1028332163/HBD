package com.esoft.wms;

import java.io.BufferedReader;
import java.io.IOException;

import javax.inject.Inject;
import javax.inject.Named;

import org.atmosphere.config.service.Disconnect;
import org.atmosphere.config.service.Get;
import org.atmosphere.config.service.ManagedService;
import org.atmosphere.config.service.Message;
import org.atmosphere.config.service.Post;
import org.atmosphere.config.service.Ready;
import org.atmosphere.config.service.Resume;
import org.atmosphere.cpr.AtmosphereResource;
import org.atmosphere.cpr.AtmosphereResourceEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.atmosphere.cpr.Broadcaster;

@ManagedService(path = "/test")
public class TestHandler {
	private final Logger logger = LoggerFactory.getLogger(TestHandler.class);

    @Inject
    @Named("/chat")
    private Broadcaster broadcaster;
	
	

	public Broadcaster getBroadcaster() {
		return broadcaster;
	}

	public TestHandler() {
		logger.info("construct was invoked");
	}

	@Ready
	public void onReady(AtmosphereResource resource) {
		broadcaster.addAtmosphereResource(resource);
		resource.session().setAttribute("projectId", "1");
		logger.info("onReady: "+ resource.uuid());
	}

	@Message
	public void onMessage(AtmosphereResource r) {
		logger.info("onMessage:" + r.uuid());
	}

	@Get
	public void onGet(AtmosphereResource r) {
		logger.info("onGet:" + r.uuid());
	}

	@Post
	public void onPost(AtmosphereResource resource) {
		logger.info("onPost:"+ resource.uuid());
		logger.info("uuid is isSuspended: " + resource.isSuspended());
		logger.info("uuid is isResumed: " + resource.isResumed());
		logger.info("uuid is isCancelled: " + resource.isCancelled());
		String message = "";
		try {
			BufferedReader bf = resource.getRequest().getReader();
			String line = bf.readLine();
			while (line != null) {
				message += line;
				line = bf.readLine();
			}
		} catch (IOException e1) {
			logger.error("read message from recource fail ", message);
		}
		logger.info(message);
	}
	@Resume
	public void onResume(AtmosphereResource resource) {
		logger.info("onResume ");
		logger.info("uuid is isSuspended: " + resource.isSuspended());
		logger.info("uuid is isResumed: " + resource.isResumed());
		logger.info("uuid is isCancelled: " + resource.isCancelled());
	}

	@Disconnect
	public void onDisconnect(AtmosphereResourceEvent event) {
		AtmosphereResource resource = event.getResource();
		String projectId = (String)resource.session().getAttribute("projectId");
		System.out.println(projectId);
		if (event.isCancelled()) {
			logger.info("Browser {} unexpectedly disconnected", resource.uuid());
		} else if (event.isClosedByClient()) {
			logger.info("Browser {} closed the connection", resource.uuid());
		}
		logger.info("uuid is isSuspended: " + resource.isSuspended());
		logger.info("uuid is isResumed: " + resource.isResumed());
		logger.info("uuid is isCancelled: " + resource.isCancelled());
	}

}
