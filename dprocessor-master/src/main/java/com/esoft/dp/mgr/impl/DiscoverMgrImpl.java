package com.esoft.dp.mgr.impl;

import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.esoft.dp.mgr.DiscoverMgr;

public class DiscoverMgrImpl implements DiscoverMgr{

	private static Logger logger = LoggerFactory
			.getLogger(DiscoverMgrImpl.class);

	@Override
	public boolean discover(String target, String source) {
		// source 下是否有未处理的文件夹
		if(null != FileUtils.listFiles(new File(source), 
									new String[]{"csv"}, true)){
			try {
				FileUtils.copyDirectoryToDirectory(new File(source), new File(target));
			} catch (IOException e) {
				logger.error(e.getMessage());
			}
			return true;
		}
		return false;
	}

}
