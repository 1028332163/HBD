package com.esoft.wms.log.sset;

import org.apache.commons.io.input.Tailer;
import org.atmosphere.cpr.AtmosphereResource;

/**
 * 
 * @author taos
 * 组合session 对应的 tailer Resource LogResource
 *
 */
public class LogSet {
	
	private Tailer tailer;
	
	private AtmosphereResource resource;
	
	private LogResource lr;
	
	public Tailer getTailer() {
		return tailer;
	}

	public void setTailer(Tailer tailer) {
		this.tailer = tailer;
	}

	public AtmosphereResource getResource() {
		return resource;
	}

	public void setResource(AtmosphereResource resource) {
		this.resource = resource;
	}

	public LogResource getLr() {
		return lr;
	}

	public void setLr(LogResource lr) {
		this.lr = lr;
	}
}
