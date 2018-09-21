package com.esoft.wms.utils;


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
	
	public static String trimQuotes(String content) {
		return content.replaceAll("\"","");
	}
	
	public static String checkQuotes(String content) {
		if(content.indexOf("\"") != -1){
			return "\"" + trimQuotes(content) + "\"";
		}
		return "\"" + content + "\"";
	}

	public static void main(String[] args) {
		String ss = "\"" + "190298289" + "\"";
		System.out.println(checkQuotes(ss));

	}
	/**
	 * 方法�?要描述信�?.
	 * <p>
	 * 描述 : 将字符串中的html转义字符进行转义
	 * </p>
	 * <p>
	 * 备注 :
	 * </p>
	 * 
	 * @param srcStr
	 *            含有html要转义的字符的字符串
	 * @return String
	 */
	public static String decodeHtml(String srcStr) {
		/**
		 * html要转义的字符 HTML 源代�? 显示结果 描述 &lt; < 小于号或显示标记 &gt; > 大于号或显示标记 &amp; &
		 * 可用于显示其它特殊字�? &quot; " 引号 &reg; ® 已注�? &copy; © 版权 &trade; �? 商标
		 * &ensp; 半个空白�? &emsp; �?个空白位 &nbsp; 不断行的空白
		 */
		srcStr = srcStr.replaceAll("<", "&lt;");
		srcStr = srcStr.replaceAll(">", "&gt;");
//		srcStr = srcStr.replaceAll("&", "&amp;");
//		srcStr = srcStr.replaceAll("\"", "&quot;");
//		srcStr = srcStr.replaceAll("®", "&copy;");
//		srcStr = srcStr.replaceAll("®", "&reg;");
//		srcStr = srcStr.replaceAll("�?", "&trade;");
		//srcStr = srcStr.replaceAll(" ", "&nbsp;");
		srcStr = srcStr.replaceAll(System.getProperty("line.separator"),
				"<br/>");
		return srcStr;
	}

}
