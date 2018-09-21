package com.esoft.wms;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Timer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.atmosphere.cpr.ApplicationConfig;
import org.atmosphere.cpr.AtmosphereResource;
import org.atmosphere.nettosphere.Nettosphere;
import org.atmosphere.nettosphere.Config.Builder;
import org.springframework.context.support.FileSystemXmlApplicationContext;

import com.esoft.wms.handler.EndJobHandler;
import com.esoft.wms.utils.Config;

public class WMSMain {

	public static FileSystemXmlApplicationContext context;
	//保证线程安全
	public static ConcurrentMap<String, AtmosphereResource> linkResources = new ConcurrentHashMap<String, AtmosphereResource>();

	public static void main(String[] args) {
		if (args.length > 0) {
			context = new FileSystemXmlApplicationContext(args[0]);
			BufferedReader reader = new BufferedReader(new InputStreamReader(
					System.in));
			String host = Config.getValue("Nettosphere.ip");
			Integer port = Integer.parseInt(Config
					.getValue("Nettosphere.port"));			
			// 启动websocket服务
			Builder builder = new Builder().host(host).port(port);
			builder.initParam(ApplicationConfig.RECOVER_DEAD_BROADCASTER,
					"false");
			builder.initParam(ApplicationConfig.PROPERTY_SESSION_SUPPORT,
					"true");
			Nettosphere server = new Nettosphere.Builder().config(
					builder.build()).build();
			server.start();
			// 启动处理endJob的timer
			Timer timer = new Timer();
			EndJobHandler endJobHandler = (EndJobHandler) context
					.getBean("endJobHandler");
			timer.scheduleAtFixedRate(endJobHandler, 0, 10 * 1000);

			while (true) {
				try {
					String text = reader.readLine();
					if ("stop".equalsIgnoreCase(text))
						break;
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			timer.cancel();
			server.stop();
			context.close();
		} else {
			System.out.println("Need spring context configuration file!");
		}
		System.exit(0);
	}

}
