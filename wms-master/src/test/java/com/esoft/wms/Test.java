package com.esoft.wms;


public class Test {

	public static void main(String[] args) throws Throwable {
		String s = "\"\\\"";
		System.out.println(s);
		System.out.println(s.replace("\\", "\\\\").replace("\"", "\\\""));
		

		// System.out.println(String.format(s, "1","2"));
	}

}
