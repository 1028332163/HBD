package com.esoft.dp.convert;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

/**
 * 输入输出数据转换
 * 
 * @author taosh
 *
 */
public interface IoConvertor {

	/**
	 * 将输入数据转为实例
	 * 
	 * @param in
	 *            输入数据流
	 * @param clazz
	 *            实例对象类
	 * @return 对象实例
	 * @throws Throwable
	 */
	public Object read(InputStream in, Class<?> clazz) throws Throwable;

	/**
	 * 将字符串文本转为实例
	 * 
	 * @param content
	 *            文本内容
	 * @param clazz
	 *            实例对象类
	 * @return 对象实例
	 * @throws Throwable
	 */
	public Object read(String content, Class<?> clazz) throws Throwable;

	/**
	 * 从文本读取数组
	 * 
	 * @param content
	 *            文本内容
	 * @return 数组对象
	 * @throws Throwable
	 */
	public Object readArray(String content) throws Throwable;
	
	
	List<?> readList(String jsonArray, Class<?> clazz) throws Throwable;

	/**
	 * 将对象转换为指定类实例
	 * 
	 * @param object
	 *            目标对象
	 * @param clazz
	 *            类
	 * @return 类实例
	 * @throws Throwable
	 */
	public Object readObject(Object object, Class<?> clazz) throws Throwable;

	/**
	 * 将实例转为输出数据
	 * 
	 * @param out
	 *            输出数据流
	 * @param object
	 *            对象实例
	 * @throws Throwable
	 */
	public void write(OutputStream out, Object object) throws Throwable;

	/**
	 * 将实例转为字符串文本
	 * 
	 * @param object
	 *            对象实例
	 * @return 文本内
	 * @throws Throwable
	 */
	public String write(Object object) throws Throwable;
}
