package com.esoft.dp.utils;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ReflectUtils {
	
	private static Logger logger = LoggerFactory.getLogger(ReflectUtils.class);
	
	private ReflectUtils(){};
	
	public static void callSetMethod(Object owner, String fieldName,Object value) {
			String setName = "set" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);
			Class<?> ownerClass = owner.getClass();
			try {
				Field field=ownerClass.getField(fieldName);
				Method method = ownerClass.getMethod(setName,field.getType());
				method.invoke(owner,value);
			} catch (Exception e) {
				logger.error("callSetMethod:", e);
			}
	}
	
	public static Object invokeMethod(Object owner, String methodName,Object[] args) {
			Class<?> ownerClass = owner.getClass();
			Class<?>[] argsClass = null;
			if (args != null && args. length != 0) {
				argsClass = new Class[args. length];
				for ( int i = 0; i < args. length; i++) {
					if(args[i] == null) return null;
					argsClass[i] = args[i].getClass();
				}
			}
			try {
				Method method = ownerClass.getMethod(methodName, argsClass);
				return method.invoke(owner, args);
			} catch (SecurityException e) {
				logger.error("invokeMethod:",e);
			} catch (NoSuchMethodException e) {
				logger.error("invokeMethod:",e);
			} catch (Exception ex) {
				logger.error("invokeMethod:",ex);
			}
			return null;
	}

}
