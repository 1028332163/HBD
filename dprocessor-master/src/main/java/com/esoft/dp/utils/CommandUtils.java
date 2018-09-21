package com.esoft.dp.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author taoshi 执行bat工具类
 */
public class CommandUtils {

	private static Logger logger = LoggerFactory.getLogger(CommandUtils.class);
	public static final String JAVA_TYPE_INTEGER = "java.lang.Integer";
	public static final String JAVA_TYPE_LONG = "java.lang.Long";

	// private CommandUtils(){}

	public static void exeCmdByPath(String path) {
		try {
			Process process = Runtime.getRuntime().exec(path);
			InputStream in = process.getInputStream();
			// 封装输入流
			BufferedReader reader = new BufferedReader(
					new InputStreamReader(in));
			String line = null;
			// 逐行读取信息
			while ((line = reader.readLine()) != null) {
				logger.info(line);
			}
			process.getOutputStream().close();
			int exitValue = process.waitFor();
			logger.info("Return Value:" + exitValue);
			process.destroy();
		} catch (InterruptedException ie) {
			logger.error(ie.getMessage());
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
	}

	/**
	 * 2. * Description: 向FTP服务器上传文件 3.* @Version1.0 4.* @param url
	 * FTP服务器hostname 5. * @param port FTP服务器端口 6.* @param username FTP登录账号 7.* @param
	 * password FTP登录密码 8.* @param path FTP服务器保存目录 9.* @param filename
	 * 上传到FTP服务器上的文件名 10.* @param input 输入流
	 * 
	 * @return 成功返回true，否则返回false
	 */
	public static boolean uploadFile(String url, int port, String username,
			String password, String filename, InputStream input) {
		boolean success = false;
		FTPClient ftp = new FTPClient();
		try {
			int reply;
			ftp.connect(url);// 连接FTP服务器
			// 如果采用默认端口，可以使用ftp.connect(url)的方式直接连接FTP服务器
			ftp.login(username, password);// 登录
			reply = ftp.getReplyCode();
			if (!FTPReply.isPositiveCompletion(reply)) {
				ftp.disconnect();
				return success;
			}
			filename = new String(filename.getBytes("GBK"), "ISO-8859-1");
			ftp.storeFile(filename, input);

			input.close();
			ftp.logout();
			success = true;
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (ftp.isConnected()) {
				try {
					ftp.disconnect();
				} catch (IOException ioe) {
				}
			}
		}
		return success;
	}

	/**
	 * 方法:生成UUID
	 * 
	 * @author：taosh
	 * @return String
	 */
	public static String generUUID() {
		String s = UUID.randomUUID().toString();
		return s.substring(0, 8) + s.substring(9, 13) + s.substring(14, 18)
				+ s.substring(19, 23) + s.substring(24);

	}

	/**
	 * @author taoshi
	 * @param str
	 *            传进来处理字符串
	 * @param length
	 *            字符串要补位的长度
	 * @param paramType
	 *            传递进来字段的类型
	 * @return
	 */
	public static String checkStrBlank(Object str, Integer length,
			Class<?> paramType) {
		StringBuilder fillStr = new StringBuilder("");
		String appendStr = " ";
		if (null != str) {
			if (str instanceof Integer || str instanceof Long) {
				appendStr = "0";
				return leftPad(str.toString(), length, appendStr);
			}
			if (null != str && StringUtils.isNotEmpty(str.toString())) {
				return rightPad(str.toString(), length, appendStr);
			}
		} else {
			if (paramType.getName().equals(JAVA_TYPE_INTEGER)
					|| paramType.getName().equals(JAVA_TYPE_LONG)) {
				appendStr = "0";
			}
			for (int i = 0; i < length; i++) {
				fillStr.append(appendStr);
			}
			return fillStr.toString();
		}
		return null;
	}

	/**
	 * modify by taoshi,超长情况需要截取
	 * 
	 * @param src
	 * @param totalLength
	 * @param appendStr
	 * @return
	 */
	public static String leftPad(String src, int totalLength, String appendStr) {
		try {
			byte[] tmp = src.getBytes("GBK");
			int needLength = totalLength - tmp.length;
			return StringUtils.leftPad(src, needLength + src.length(),
					appendStr);
		} catch (UnsupportedEncodingException e) {
			logger.error("leftPad", e);
		}
		return null;
	}

	/**
	 * modify by taoshi,超长情况需要截取
	 * 
	 * @param src
	 * @param totalLength
	 *            ：限制字节长度
	 * @param appendStr
	 * @return
	 */
	public static String rightPad(String src, int totalLength, String appendStr) {
		try {
			byte[] tmp = src.getBytes("GBK");
			// 输入汉字较多，大于要求字节数
			if (tmp.length > totalLength) {
				byte[] newByte = Arrays.copyOfRange(tmp, 0, totalLength - 1);
				int newStringSize = new String(newByte, "GBK").length();
				//rightPad 第二个参数是要增加至多长
				//字符串长度+总长度-字节数组长度
				return StringUtils
						.rightPad(new String(newByte, "GBK"), newStringSize
								+ totalLength - newByte.length, appendStr);
			} else {
				int needLength = totalLength - tmp.length;
				return StringUtils.rightPad(src, needLength + src.length(),
						appendStr);
			}
		} catch (UnsupportedEncodingException e) {
			logger.error("leftPad", e);
		}
		return null;
	}

	// public static String rightIndexOutPad(String src, int totalLength, String
	// appendStr){
	//
	// }

	public static String checkDoubleBlank(Double data, Integer length,
			Integer count) {
		if (data == null)
			return checkStrBlank(null, length, java.lang.Integer.class);
		BigDecimal big = new BigDecimal(data.toString());
		if (1 == count) {
			big = big.multiply(new BigDecimal(10));
		} else if (2 == count) {
			big = big.multiply(new BigDecimal(100));
		}
		return checkStrBlank(big.intValue(), length, java.lang.Integer.class);
	}

	/**
	 * @author taoshi 传入：LIU NENGNENG 转换为：LIU/NENGNENG/
	 * @return
	 */
	public static String reWrapC4BName(String name) {
		if (StringUtils.isNotBlank(name)) {
			StringBuilder newName = new StringBuilder("");
			String[] names = name.split(" ");
			for (String str : names) {
				newName.append(str);
				newName.append("/");
			}
			return newName.toString();
		}
		return name;
	}

	/**
	 * @author taoshi 校验输入内容是否为数字
	 * @param str
	 * @return
	 */
	public static boolean checkNumber(char str) {
		if (str < 48 || str > 57) {
			return false;// 不是数字
		}
		return true;
	}

	/**
	 * 校验字符数组，从最后一位开始，如果最后一位为数字，则返回偏移量 如果最后不是数字，返回0
	 * 
	 * @param arr
	 * @return
	 */
	public static int checkNumber(String str) {
		char[] arr = str.toCharArray();
		if (checkNumber(arr[arr.length - 1])) {// 如果最后一位不是数字，直接返回0
			for (int i = arr.length; --i >= 0;) {
				if (!checkNumber(arr[i])) {
					return i + 1 - arr.length;
				}
			}
		}
		return 0;
	}

	/**
	 * @author:taoshi 从字符串中截取多少字节的内容返回
	 * @param str
	 * @param length
	 * @return
	 * @throws UnsupportedEncodingException
	 */
	public static String subByByteLength(String str, int begin, int end)
			throws UnsupportedEncodingException {
		// return new String(Arrays.copyOfRange(str.getBytes("GBK"),begin,
		// end));
		return str.substring(begin, end);
	}

	/**
	 * @author taoshi 重新装配省市区的字符串
	 * @return
	 * @throws UnsupportedEncodingException
	 */
	public static String[] reWrapperStr(String[] str, String allStr,
			String distStr) throws UnsupportedEncodingException {
		int amount = 0;// 第二段截取偏移量
		int amountThird = 0;
		int distLength = distStr.length();// 区 长度 字节
		int totalLength = allStr.length();// 路/街 +号+详细 的长度
		if (distLength < 15 && totalLength >= (15 - distLength)) {// 区小于15,pad
																	// 来的之后的位数大于15
																	// - 区长度
			// 判断最后一位是否为数字，如果是数字，要保证数字在一起，换行的位置要往前移动
			amount = checkNumber(CommandUtils.subByByteLength(
					allStr.toString(), 0, 15 - distLength));// 计算偏移量，如果结尾有数字，则前后偏移
			str[1] = distStr
					+ CommandUtils.subByByteLength(allStr, 0, 15 - distLength
							+ amount);
			if (totalLength >= (15 - distLength) + 15) {// 如果2+3+4 大于 第二位剩余长度+30
				amountThird = checkNumber(CommandUtils.subByByteLength(allStr,
						0, 30 - distLength + amount));
				str[2] = CommandUtils.subByByteLength(allStr.toString(), 15
						- distLength + amount, 30 - distLength + amount
						+ amountThird);
				if (totalLength >= (15 - distLength) + 30) {// 如果2+3+4 大于
															// 第三位剩余长度+30

					str[3] = CommandUtils.subByByteLength(allStr.toString(), 30
							- distLength + amountThird + amount, 45
							- distLength + amountThird + amount);
				} else {

					str[3] = CommandUtils.subByByteLength(allStr.toString(), 30
							- distLength + amountThird, totalLength
							+ amountThird);
				}
			} else {// 2+3+4 大于第二位剩余长度 而小于 第三位长度
				str[2] = CommandUtils.subByByteLength(allStr.toString(), 15
						- distLength + amount, totalLength);
			}

		} else {// 如果2+3+4 长度小于 第二位剩余长度
			str[1] = distStr
					+ CommandUtils.subByByteLength(allStr.toString(), 0,
							totalLength);
		}
		return str;

	}

	public static void main(String args[]) {
		// String s =
		// "                                                    哼哼唧唧";
		// try {
		// System.out.println(s.getBytes("GBK").length);
		// } catch (UnsupportedEncodingException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }
		// System.out.println("=="+checkStrBlank(null,
		// 10,java.lang.Integer.class)+"=");
		// System.out.println("=="+checkStrBlank(null,
		// 10,java.lang.String.class)+"=");
		// System.out.println("=="+checkDoubleBlank(null, 8, 1)+"=");
		// System.out.println("=="+checkDoubleBlank(8.9, 8, 2)+"=");
		// System.out.println("=="+checkStrBlank("哼哼唧唧",
		// 10,java.lang.String.class)+"=");
		// System.out.println("=="+checkStrBlank("89",
		// 10,java.lang.Integer.class)+"=");
		// System.out.println(reWrapC4BName("LIU NENGNENG"));
		// System.out.println(checkNumber("1"));
		// String str = "aa12";
		// System.out.println(checkNumber(str));
		// int a = -1;
		// System.out.println(1-a);
		// BigDecimal b = new BigDecimal(92.3);
		// System.out.println(b.intValue()+"");
		// String[] str = new String[4];
		// str[0] = "北京市";
		// String allStr = "毛楼街道股嘎嘎哇咔11大街38号";
		// try {
		// String[] strs = reWrapperStr(str, allStr, "东城区");
		// for (int i = 0; i < strs.length; i++) {
		// System.out.println(strs[i]);
		// }
		//
		// } catch (UnsupportedEncodingException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }
		try {
			System.out.println("=="
					+ checkStrBlank("符合烦得", 30,
							java.lang.String.class).getBytes("gbk").length
					+ "=");
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("=="
				+ checkStrBlank("符合烦得", 30,
						java.lang.String.class) + "=");
	}
}
