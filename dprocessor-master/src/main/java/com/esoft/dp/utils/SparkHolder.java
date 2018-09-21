//package com.esoft.dp.utils;
//
//import org.apache.spark.SparkConf;
//import org.apache.spark.SparkContext;
//
//public class SparkHolder {
//    private static volatile SparkHolder instance;
//    private SparkContext sparkContext;
//    private SparkConf sparkConf;
//
//
//    public static SparkHolder getInstance() {
//        if (instance == null) {
//            synchronized (SparkHolder.class) {
//                if (instance == null) {
//                    instance = new SparkHolder();
//                }
//            }
//        }
//        return instance;
//    }
//
//
//    public SparkConf getSparkConf() {
//        return sparkConf;
//    }
//
//    public void setSparkConf(SparkConf sparkConf) {
//        this.sparkConf = sparkConf;
//    }
//
//    public SparkContext getSparkContext() {
//        return sparkContext;
//    }
//
//    public void setSparkContext(SparkContext sparkContext) {
//        this.sparkContext = sparkContext;
//    }
//}
