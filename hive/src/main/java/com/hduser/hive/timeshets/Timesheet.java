package com.hduser.hive.timeshets;

import static org.apache.spark.sql.functions.col;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import com.hduser.hive.config.Conf;
import com.hduser.hive.incomes.Period;
public class Timesheet {

	private Dataset<Row> timesheet_period;

	public Timesheet() {

	}

	public Timesheet(String start_date, String end_date) {
		getTimesheetPeriod(start_date, end_date);

	}

	public Dataset<Row> getTimesheetPeriod(String start_date, String end_date) {

		String sql_get_time_sheet_period = 
				"select * from TA_EMPLOYEE_TIMESHEETS as Sheet "
				+ "join TA_WORKING_SHIFTS as Shift on Sheet.ACTUAL_SHIFT_ID = Shift.WORKING_SHIFT_ID "
				+ "join TA_WORKING_TYPES as Type on Sheet.APPROVED_WORKING_TYPE_ID =  Type.WORKING_TYPE_ID";
		this.timesheet_period = Conf.spark.sql(sql_get_time_sheet_period)
				.filter(col("WORKING_DATE").between(start_date, end_date));

		//		timesheet_period.cache();
		return timesheet_period;
	}

	public Dataset<Row> getTimesheetEmployee(int employee_id){
		Dataset<Row> timesheetEmployeeInPeriod = timesheet_period
				.filter(col("EMPLOYEE_ID").equalTo(employee_id));
		
		return timesheetEmployeeInPeriod;
	}

	public Dataset<Row> getTimesheetInNightEmployee(int employee_id){
		Dataset<Row> timesheetEmployeeInPeriod = 
				getTimesheetEmployee(employee_id)
				.filter(col("IS_NIGHT_SHIFT").equalTo(1));
		
		return timesheetEmployeeInPeriod;
	}

	public Dataset<Row> groupbyWorkingType(Dataset<Row> dataset) {
		Dataset<Row> timesheet_employee_groupby_workingtype=
				dataset.groupBy("ACTUAL_SHIFT_ID").count();
		
		return timesheet_employee_groupby_workingtype;
	}



	public static void main(String[] args) {
		int period_id = 89;
		int employee_id = 5170;
		String start_date = Period.getStartDate(period_id);
		String end_date = Period.getEndDate(period_id);
		
		Timesheet timesheet = new Timesheet(start_date, end_date);
		
		Dataset<Row> timesheetPeriod= timesheet.getTimesheetEmployee(employee_id);
		Dataset<Row> groupTimesheet = timesheet.groupbyWorkingType(timesheetPeriod);
		groupTimesheet.write().mode(SaveMode.Overwrite).json("/home/hduser/output/groupTimesheet");

		Conf.spark.close();
	}

}
