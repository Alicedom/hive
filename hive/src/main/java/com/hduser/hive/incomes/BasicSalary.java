package com.hduser.hive.incomes;

import static org.apache.spark.sql.functions.col;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import com.hduser.hive.config.Conf;

public class BasicSalary {


	private Dataset<Row> basicSalary;

	public BasicSalary() {

	}

	public BasicSalary(int employee_id, int period_id) {

		getBasicSalary(period_id);
		getBasicSalaryPerEmployee(employee_id);
	}

	public Dataset<Row> getBasicSalary(int period_id) {
		String start_date = Period.getStartDate(period_id);
		String end_date = Period.getEndDate(period_id);

		/*
		 * start_date (end_date) < END_DATE (end_date)
		 * and
		 * (start_date) START_DATE (start_date) < end_date
		 * and
		 * start_date < end_date
		 */
		String sql_get_basicsalary = "select * from INCOME_CONFIGS where INCOME_ID = 46" ;
		Dataset<Row> basicSalaryPeriod = Conf.spark.sql(sql_get_basicsalary)
				.filter(col("END_DATE").geq(start_date))
				.filter(col("START_DATE").leq(end_date))
//				.select("EMPLOYEE_ID", "INCOME_CONFIG_ID", "CUSTOM_VALUE")
				;
//		basicSalaryPeriod.write().mode(SaveMode.Overwrite).json("/home/hduser/out/basicSalaryPeriod");
		Dataset<Row> basicSalaryMax = basicSalaryPeriod
				.groupBy("EMPLOYEE_ID")
				.max("INCOME_CONFIG_ID")
				.withColumnRenamed("EMPLOYEE_ID", "Max_ EMPLOYEE_ID")
//				.repartition(4)
				;

//		basicSalaryMax.write().mode(SaveMode.Overwrite).json("/home/hduser/out/basicSalaryMax");
		basicSalary = basicSalaryMax
				.join(basicSalaryPeriod)
				.where(col("max(INCOME_CONFIG_ID)").equalTo(col("INCOME_CONFIG_ID")))
				.select("EMPLOYEE_ID","INCOME_ID", "CUSTOM_VALUE")
				.orderBy("EMPLOYEE_ID")
				.repartition(4)
				;
		
		basicSalary.cache();
		basicSalary.write().mode(SaveMode.Overwrite).json("/home/hduser/out/basicSalary1");
//		System.out.println("number "+basicSalary.count());
		return basicSalary;
	}

	public double getBasicSalaryPerEmployee(int employee_id) {
		Dataset<Row> basicSalaryPerEmployee=
				basicSalary.filter(col("EMPLOYEE_ID").equalTo(employee_id));
		//		basicSalaryPerEmployee.show();
		basicSalaryPerEmployee.write().mode(SaveMode.Overwrite).json("/home/hduser/out/basicSalaryPerEmployee");
		double basicSalaryPerEmployeeFirst=
				basicSalaryPerEmployee
				.select("CUSTOM_VALUE")
				.as(Encoders.DOUBLE())
				.first();

		System.out.println(basicSalaryPerEmployeeFirst);
		return basicSalaryPerEmployeeFirst;
	}
	public static void main(String[] args) {
		int period_id = 89;
		int employee_id = 5170;

		new BasicSalary(employee_id, period_id);

		Conf.spark.close();

	}

}
