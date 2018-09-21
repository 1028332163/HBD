package com.esoft.dp.utils;

public class HDFSIOException extends AncestorsException {
  /**
	 * 
	 */
	private static final long serialVersionUID = -3544410528124532642L;

	public HDFSIOException(String hdfsURI, String hdfsConf, Exception e) {
    super( "HDFS IO Failure: \n"
            + " accessed URI : " + hdfsURI + "\n"
            + " configuration: " + hdfsConf + "\n"
            + " " + e);
    }
}
