package com.hduser.hive.config;

import org.apache.spark.sql.SparkSession;

public class Conf {
	public static String connectURL ="jdbc:derby:;databaseName=/home/hduser/derby_folder/metastore_db;create=true";
	public static SparkSession spark = SparkSession
			  .builder()
			  .master("local[*]")
			  .appName("Java Spark Hive")
			  .config("javax.jdo.option.ConnectionURL",connectURL)
			  .enableHiveSupport()
			  .getOrCreate();

}
