package com.esoft.dp;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.springframework.context.support.FileSystemXmlApplicationContext;
import org.quartz.*; 
import com.esoft.dp.quartz.ServiceOpener;

/**
 * 程序入口
 *
 */
public class DPMain 
{
	public static void main(String[] args) throws SchedulerException {
		//设置系统的时区和国家
		System.setProperty("user.timezone","Asia/Shanghai"); 
		System.setProperty("user.country","CN");
		
		if (args.length > 0) {
//			System.setProperty("org.quartz.properties", "server.properties");
//			System.setSecurityManager(new RMISecurityManager());
			final FileSystemXmlApplicationContext context = new FileSystemXmlApplicationContext(
					args[0]);

/////////////////////////////////////////创建socket服务		
			ServiceOpener modifyServiceOpener = (ServiceOpener)
					context.getBean("serviceOpener");
			modifyServiceOpener.openService();		

			BufferedReader reader = new BufferedReader(new InputStreamReader(
					System.in));
			while (true) {
				try {
					String text = reader.readLine();
					if ("stop".equalsIgnoreCase(text))
						break;
				} catch (IOException e) {
				}
			}
//			context.close();
		} else {
			System.out.println("Need spring context configuration file!");
		}
		System.exit(0);
	}

}
