package com.esoft.dp;

public class Test {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String cmd = "/opt/cloudera/parcels/CDH-5.7.1-1.cdh5.7.1.p0.11/lib/spark/bin/spark-submit --conf spark.executor.memory=2g --conf spark.driver.memory=2g --num-executors 2 --master yarn-client --queue default --driver-class-path /root/dptest/dprocessor/task/json4s-native_2.10-3.2.10.jar:/root/dptest/dprocessor/task/json4s-core_2.10-3.2.10.jar:/root/dptest/dprocessor/task/mysql-connector-java-5.1.34.jar --class com.esoft.dae.energyAlgorithm.ml.LinearRegressionML /root/dptest/dprocessor/task/dae-1.0-SNAPSHOT.jar hdfs://192.168.88.10:8020/ /out/20161122/p269_1479791941596.parquet 1 100 0.000001 \"\" \"`numpregnancies`\" \"/out/20161122/m2691479792096416\" /out/20161122/p269_1479791975898.parquet 68";
		System.out.println(cmd.substring(cmd.lastIndexOf(" ")+1, cmd.length()));
	}

}
