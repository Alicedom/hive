package com.hduser.hive.incomes;

import com.hduser.hive.config.Conf;

public class TotalStat {
	public TotalStat(String employee_id, String period_id) {
		String sql = "select * from ";
		Conf.spark.sql(sql);
	}
	

}
