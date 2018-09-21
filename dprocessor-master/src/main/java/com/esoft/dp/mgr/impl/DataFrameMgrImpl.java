//package com.esoft.dp.mgr.impl;
//
//import org.apache.spark.sql.DataFrame;
//import org.apache.spark.sql.DataFrameReader;
//import org.apache.spark.sql.SQLContext;
//import com.esoft.dp.mgr.DataFrameMgr;
//import com.esoft.dp.utils.SparkHolder;
//
//public class DataFrameMgrImpl implements DataFrameMgr{
//	
//	@Override
//	public DataFrame toDataFrame(String csv) {
//		DataFrameReader reader = new SQLContext(SparkHolder.getInstance().getSparkContext()).read();
//		return reader.load(csv);
//	}
//
//}
