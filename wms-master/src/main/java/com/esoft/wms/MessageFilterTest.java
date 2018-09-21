package com.esoft.wms;

import org.atmosphere.cpr.AtmosphereResource;
import org.atmosphere.cpr.BroadcastFilter.BroadcastAction.ACTION;
import org.atmosphere.cpr.PerRequestBroadcastFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageFilterTest implements PerRequestBroadcastFilter{
	private final Logger logger = LoggerFactory.getLogger(MessageFilterTest.class);
	@Override
	public BroadcastAction filter(String broadcasterId, Object originalMessage,
			Object message) {
		logger.info("filter1 has invoke ");
		logger.info(originalMessage.toString());
		return new BroadcastAction(ACTION.ABORT,message);
	}

	@Override
	public BroadcastAction filter(String broadcasterId, AtmosphereResource r,
			Object originalMessage, Object message) {
		logger.info("filter2 has invoke ");
		logger.info(originalMessage.toString()+"to"+r.uuid());
		return new BroadcastAction(message);
	}

}
