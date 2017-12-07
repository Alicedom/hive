package com.hduser.hive.timeshets;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.hduser.hive.config.Conf;
import com.hduser.hive.incomes.IncomeConfig;
import com.hduser.hive.incomes.Period;

public class LuongTimeSheet {

	public LuongTimeSheet() {
	}

	public Dataset<Row> getWorkingType() {
		String sql_get_working_type = "select * from TA_WORKING_TYPE";
		return Conf.spark.sql(sql_get_working_type);
	}

	public void getLuongTimesheet(){

	}
	public static void main(String[] args) {
		int period_id = 89;
		int employee_id = 5170;
		int income_id = 46; //Basic Salary Id
		String start_date = Period.getStartDate(period_id);
		String end_date = Period.getEndDate(period_id);


		Timesheet timesheet = new Timesheet(start_date, end_date);
		Dataset<Row> timesheetPeriod= timesheet.getTimesheetEmployee(employee_id);
		Dataset<Row> groupTimesheet = timesheet.groupbyWorkingType(timesheetPeriod);

		
		IncomeConfig incomeConfig = new IncomeConfig(start_date, end_date);
		Dataset<Row> incomeConfigsPerEmployee = incomeConfig.getIncomeConfigPerEmployee(employee_id);
		double basicSalary = incomeConfig.getIncome(income_id);


	}
}
