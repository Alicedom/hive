package com.hduser.hive.timeshets;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;

import com.hduser.hive.config.Conf;

import static org.apache.spark.sql.functions.col;
public class TimeSheet {

	private String employee_id;
	private int period_id;
	private String start_date;
	private String end_date;
	private Dataset<Row> timesheet_period;

	public TimeSheet() {

	}

	public TimeSheet(String employee_id, int period_id) {

		this.employee_id = employee_id;
		this.period_id = period_id;
		this.start_date = getStartDate(period_id);
		this.end_date = getEndDate(period_id);
		this.timesheet_period = getTimesheetPeriod(period_id);
	}

	public Dataset<Row> getTimesheetEmployeeInPeriod(Dataset<Row> timesheet_period, int employee_id){
		Dataset<Row> timesheetEmployeeInPeriod = null;


		return timesheetEmployeeInPeriod;
	}

	public Dataset<Row> getTimesheetPeriod(int period_id) {
		if(this.start_date == null)
			this.start_date = getStartDate(period_id);
		if(this.end_date == null)
			this.end_date = getEndDate(period_id);

		String sql_get_time_sheet_period = "select * from TA_EMPLOYEE_TIMESHEETS ";
		this.timesheet_period = Conf.spark.sql(sql_get_time_sheet_period).filter(col("WORKING_DATE").between(this.start_date, this.end_date));
		timesheet_period.cache();

		timesheet_period.show();
		timesheet_period.write().format("csv").option("header", "true").save("/home/hduser/timesheet_period1");
		timesheet_period.write().csv("/home/hduser/timesheet_period2");
		return timesheet_period;
	}

	public String getStartDate(int period_id) {
		String sql_get_start_date= "select START_DATE from PERIODS where PERIOD_ID = "+period_id; 

		String start_date= Conf.spark.sql(sql_get_start_date).as(Encoders.STRING()).first();
//		start_date.show();

		System.out.println(start_date);
		return start_date;
	}

	public String getEndDate(int period_id) {

		String sql_get_end_date="select END_DATE from PERIODS where PERIOD_ID = "+period_id;

		String end_date= Conf.spark.sql(sql_get_end_date).as(Encoders.STRING()).head();
//		end_date.show();

		System.out.println(end_date);
		return end_date;
	}

	public static void main(String[] args) {
		int period_id = 89;
		TimeSheet timesheet = new TimeSheet();

		timesheet.getTimesheetPeriod(period_id);
		Conf.spark.close();
	}

}