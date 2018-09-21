package com.esoft.dp.utils;

/**
 * Ancestor 异常
 * 
 */
abstract public class AncestorsException extends RuntimeException {

  /**
	 * 
	 */
  private static final long serialVersionUID = 6245417363896568989L;
	
  public AncestorsException(String s, Throwable t) {
    super(s,t);
  }
  public AncestorsException(String s) {
    super(s);
  }
}
