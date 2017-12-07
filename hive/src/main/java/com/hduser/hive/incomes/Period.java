package com.hduser.hive.incomes;

import org.apache.spark.sql.Encoders;

import com.hduser.hive.config.Conf;

public class Period {

	
	public static String getStartDate(int period_id) {
		String sql_get_start_date= "select START_DATE from PERIODS where PERIOD_ID = "+period_id; 
		String start_date= Conf.spark.sql(sql_get_start_date).as(Encoders.STRING()).first();
		
		return start_date;
	}

	public static  String getEndDate(int period_id) {
		String sql_get_end_date="select END_DATE from PERIODS where PERIOD_ID = "+period_id;
		String end_date= Conf.spark.sql(sql_get_end_date).as(Encoders.STRING()).head();

		return end_date;
	}
	public static void main(String[] args) {
		System.out.println(Period.getStartDate(89));
		System.out.println(Period.getEndDate(89));;
	}

}
