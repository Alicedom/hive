package com.hduser.hive.test;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class HDFSload {

	public static void main(String[] args) {
		SparkSession spark = SparkSession
      		  .builder()
      		  .master("local[*]")
      		  .appName("Java Spark Hive Example")
      		  .getOrCreate();
		
		Dataset<String> lines =spark.read().textFile("hdfs://localhost:54310/user/hduser/EMPLOYEES/*");
		lines.show(10);
      
	}
}