package com.hduser.hive.incomes;

import static org.apache.spark.sql.functions.col;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import com.hduser.hive.config.Conf;

public class IncomeConfig {

	private Dataset<Row> incomeConfigs;
	
	public IncomeConfig(int employee_id, int period_id) {
		getIncomeConfig(period_id);
		
	}
	
	/*
	 * start_date (end_date) < END_DATE (end_date)
	 * and
	 * (start_date) START_DATE (start_date) < end_date
	 * and
	 * start_date < end_date
	 */
	public Dataset<Row> getIncomeConfig(int period_id) {
		String start_date = Period.getStartDate(period_id);
		String end_date = Period.getEndDate(period_id);
//???????
//		String sql_get_income_configs = "select EMPLOYEE_ID, INCOME_ID, CUSTOM_VALUE, INCOME_CONFIG_ID from INCOME_CONFIGS where END_DATE >= "+start_date+ " AND  START_DATE <= "+end_date;
//		Dataset<Row> incomeConfigsPeriod = Conf.spark.sql(sql_get_income_configs);
		
		String sql_get_income_configs = "select EMPLOYEE_ID, INCOME_ID, CUSTOM_VALUE, INCOME_CONFIG_ID from INCOME_CONFIGS";
		Dataset<Row> incomeConfigsPeriod = Conf.spark.sql(sql_get_income_configs)
				.filter(col("END_DATE").geq(start_date))
				.filter(col("START_DATE").leq(end_date));
		
		
		Dataset<Row> incomeConfigsMax = incomeConfigsPeriod
				.groupBy("EMPLOYEE_ID", "INCOME_ID")
				.max("INCOME_CONFIG_ID")
				.withColumnRenamed("EMPLOYEE_ID", "Max_EMPLOYEE_ID")
				.withColumnRenamed("INCOME_ID", "Max_INCOME_ID")
				;

		incomeConfigs = incomeConfigsMax
				.join(incomeConfigsPeriod)
				.where(col("max(INCOME_CONFIG_ID)").equalTo(col("INCOME_CONFIG_ID")))
				.select("EMPLOYEE_ID","INCOME_ID", "CUSTOM_VALUE")
				.orderBy("EMPLOYEE_ID")
				.repartition(4)
				;
		
//		incomeConfigs.cache();
//		incomeConfigs.write().mode(SaveMode.Overwrite).json("/home/hduser/out/incomeConfigs1");
//		System.out.println("number "+incomeConfigs.count());
		return incomeConfigs;
	}
	
	public double getIncomeConfigPerEmployee(int employee_id) {
		Dataset<Row> incomeConfigsPerEmployee=
				incomeConfigs.filter(col("EMPLOYEE_ID").equalTo(employee_id));
		//		basicSalaryPerEmployee.show();
		incomeConfigsPerEmployee.write().mode(SaveMode.Overwrite).json("/home/hduser/out/incomeConfigsPerEmployee");
		double incomeConfigPerEmployeeFirst=
				incomeConfigsPerEmployee
				.select("CUSTOM_VALUE")
				.as(Encoders.DOUBLE())
				.first();

		System.out.println(incomeConfigPerEmployeeFirst);
		return incomeConfigPerEmployeeFirst;
	}
	
	public static void main(String[] args) {
		int period_id = 89;
		int employee_id = 5170;

		new IncomeConfig(employee_id, period_id);

		Conf.spark.close();

	}
}
