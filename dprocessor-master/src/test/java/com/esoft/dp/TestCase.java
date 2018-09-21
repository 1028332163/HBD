package com.esoft.dp;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;

public abstract class TestCase {

	public static ApplicationContext CONTEXT = null;

	public void init() {
		if (CONTEXT == null)
			CONTEXT = new FileSystemXmlApplicationContext(
					"G:\\esoftBD\\workspaces\\woso2ML1.1\\dprocessor\\conf\\test.xml");
	}

}
