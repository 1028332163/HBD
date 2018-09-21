package com.esoft.dp.utils;


public class StringHelper {
	
	private StringHelper() { /* static methods only - hide constructor */
	}
	
	public static String columnNameToProperty(String columnName){
		if (columnName.indexOf("_") != -1) {
			return removeUnderscores(columnName);
		}
		return columnName(columnName);
	}
	/**
	 * @author taoshi
	 * 移除下划线
	 * @return
	 */
	public static String removeUnderscores(String columnName){
		String[] strs = columnName.split("_");
		return columnName(strs);
	}
	/**
	 * @author taoshi
	 * 将多个单词封装为属性
	 * @param strs
	 * @return
	 */
	public static String columnName(String ...strs){
		StringBuilder result = new StringBuilder("");
		for(String string: strs){
			result.append(string.toLowerCase().substring(0, 1).toUpperCase()+ string.toLowerCase().substring(1));
		}
		return result.toString();
	}

	public static void main(String[] args) {
		System.out.println(columnNameToProperty("USER"));

	}

}
