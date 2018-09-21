package com.esoft.dp.mgr;
/**
 * 
 * @author taosh
 * 要扫描的目录，发现相关文件
 *
 */
public interface DiscoverMgr {
	
	boolean discover(String target,String source); 
}
