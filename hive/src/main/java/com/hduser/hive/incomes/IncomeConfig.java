package com.hduser.hive.incomes;

import static org.apache.spark.sql.functions.col;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import com.hduser.hive.config.Conf;

public class IncomeConfig {

	private Dataset<Row> incomeConfigs;
	private Dataset<Row> incomeConfigsPerEmployee;

	public IncomeConfig(String start_date, String end_date) {
		getIncomeConfig(start_date, end_date);

	}

	/*
	 * start_date (end_date) < END_DATE (end_date)
	 * and
	 * (start_date) START_DATE (start_date) < end_date
	 * and
	 * start_date < end_date
	 */
	public Dataset<Row> getIncomeConfig(String start_date, String end_date) {

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
		return incomeConfigs;
	}

	public Dataset<Row>  getIncomeConfigPerEmployee(int employee_id) {
		incomeConfigsPerEmployee=
				incomeConfigs.filter(col("EMPLOYEE_ID").equalTo(employee_id));

		return incomeConfigsPerEmployee;
	}
	
	public double getIncome(int income_id) {
		double incomeConfigPerEmployeeFirst=
				incomeConfigsPerEmployee
				.select("CUSTOM_VALUE")
				.where(col("INCOME_ID").equalTo(income_id))
				.as(Encoders.DOUBLE())
				.first();

		return incomeConfigPerEmployeeFirst;

	}	

	public static void main(String[] args) {
		int period_id = 89;
		int employee_id = 5170;

		int income_id = 46; //Basic Salary Id
		String start_date = Period.getStartDate(period_id);
		String end_date = Period.getEndDate(period_id);
		
		IncomeConfig incomeConfig = new IncomeConfig(start_date, end_date);
		Dataset<Row> incomeConfigsPerEmployee = incomeConfig.getIncomeConfigPerEmployee(employee_id);
		double basicSalary = incomeConfig.getIncome(income_id);
		
		incomeConfigsPerEmployee.write().mode(SaveMode.Overwrite).json("/home/hduser/output/incomeConfigsPerEmployee");
		System.out.println(basicSalary);
		Conf.spark.close();

	}
}
