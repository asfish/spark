package com.tryit.spark.launcher;

import java.util.Date;

/**
 * Created by agebriel on 6/27/17.
 */
public class JobResult
{
	private long startDate;
	private String result;
	private JobStatus jobStatus;
	public long getStartDate() {
		return startDate;
	}
	public void setStartDate(long startDate) {
		this.startDate = startDate;
	}
	public String getResult() {
		return result;
	}
	public void setResult(String result) {
		this.result = result;
	}
	public JobStatus getJobStatus() {
		return jobStatus;
	}
	public void setJobStatus(JobStatus jobStatus) {
		this.jobStatus = jobStatus;
	}
	public JobResult() {
		this.startDate = new Date().getTime();
		this.result = "";
		this.jobStatus = JobStatus.RUNNING;
	}
}
