package com.hduser.hive.timeshets;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.hduser.hive.config.Conf;

public class LuongTimeSheet {

	public LuongTimeSheet() {
	}

	public Dataset<Row> getWorkingType() {
		String sql_get_working_type = "select * from TA_WORKING_TYPE";
		return Conf.spark.sql(sql_get_working_type);
	}
	
	public void getLuongTimesheet(){
		
	}
}
