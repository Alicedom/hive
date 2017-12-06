package com.hduser.hive.timeshets;

import static org.apache.spark.sql.functions.col;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.hduser.hive.config.Conf;
import com.hduser.hive.incomes.Period;
public class CountNumberWorkingDateGroupbyWorkingType {

	private Dataset<Row> timesheet_period;

	public CountNumberWorkingDateGroupbyWorkingType() {

	}

	public CountNumberWorkingDateGroupbyWorkingType(int employee_id, String start_date, String end_date) {

		getTimesheetPeriod(start_date, end_date);
//		getTimesheetEmployeeInPeriod(employee_id);
		getCountNumberWorkingDateGroupbyWorkingType(employee_id);
	}


	public Dataset<Row> getCountNumberWorkingDateGroupbyWorkingType(int employee_id) {
		Dataset<Row> timesheetEmployeeInPeriod = getTimesheetEmployeeInPeriod(employee_id);
		Dataset<Row> timesheet_employee_period_groupby_workingtype=
				timesheetEmployeeInPeriod.groupBy("ACTUAL_SHIFT_ID").count();
		return timesheet_employee_period_groupby_workingtype;
	}

	public Dataset<Row> getTimesheetEmployeeInPeriod(int employee_id){
		Dataset<Row> timesheetEmployeeInPeriod = timesheet_period.filter(col("EMPLOYEE_ID").equalTo(employee_id));
		return timesheetEmployeeInPeriod;
	}

	public Dataset<Row> getTimesheetPeriod(String start_date, String end_date) {

		String sql_get_time_sheet_period = "select * from TA_EMPLOYEE_TIMESHEETS ";
		this.timesheet_period = Conf.spark.sql(sql_get_time_sheet_period).filter(col("WORKING_DATE").between(start_date, end_date));

//		timesheet_period.cache();

		//		timesheet_period.show();
		//		timesheet_period.write().mode(SaveMode.Overwrite).format("csv").option("header", "true").save("/home/hduser/out/timesheet_period1");
		//		timesheet_period.write().mode(SaveMode.Overwrite).csv("/home/hduser/out/timesheet_period2");
		return timesheet_period;
	}



	public static void main(String[] args) {
		int period_id = 89;
		int employee_id = 5170;
		String start_date = Period.getStartDate(period_id);
		String end_date = Period.getEndDate(period_id);
		new CountNumberWorkingDateGroupbyWorkingType(employee_id, start_date, end_date);

		//		timesheet.getTimesheetPeriod(period_id);
		Conf.spark.close();
	}

}
