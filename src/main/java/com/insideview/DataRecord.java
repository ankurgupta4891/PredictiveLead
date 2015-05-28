package com.insideview;

public class DataRecord {

	short jobLevel;
	int jobFunction;
	int empCount;
	int popularity;
	double revenue;
	boolean fortuneListed;
	String email;
	boolean label;
	int compId;
	int execId;
	int empId;
	double probability;

	public double getProbability() {
		return probability;
	}

	public void setProbability(double probability) {
		this.probability = probability;
	}

	public short getJobLevel() {
		return jobLevel;
	}

	public void setJobLevel(short jobLevel) {
		this.jobLevel = jobLevel;
	}

	public int getJobFunction() {
		return jobFunction;
	}

	public void setJobFunction(int jobFunction) {
		this.jobFunction = jobFunction;
	}

	public int getEmpCount() {
		return empCount;
	}

	public void setEmpCount(int empCount) {
		this.empCount = empCount;
	}

	public int getPopularity() {
		return popularity;
	}

	public void setPopularity(int popularity) {
		this.popularity = popularity;
	}

	public double getRevenue() {
		return revenue;
	}

	public void setRevenue(double revenue) {
		this.revenue = revenue;
	}

	public boolean isFortuneListed() {
		return fortuneListed;
	}

	public void setFortuneListed(boolean fortuneListed) {
		this.fortuneListed = fortuneListed;
	}

	public String getEmail() {
		return email;
	}

	public void setEmail(String email) {
		this.email = email;
	}

	public boolean isLabel() {
		return label;
	}

	public void setLabel(boolean label) {
		this.label = label;
	}

	public int getCompId() {
		return compId;
	}

	public void setCompId(int compId) {
		this.compId = compId;
	}

	public int getExecId() {
		return execId;
	}

	public void setExecId(int execId) {
		this.execId = execId;
	}

	public int getEmpId() {
		return empId;
	}

	public void setEmpId(int empId) {
		this.empId = empId;
	}

	public boolean isValid() {
		if ((jobFunction == 0 && jobLevel == 0) || (popularity == 0 && empCount == 0 && revenue == 0)) {
			return false;
		}
		return true;
	}

	@Override
	public String toString() {
		return "DataRecord [jobLevel=" + jobLevel + ", jobFunction=" + jobFunction + ", empCount=" + empCount + ", popularity=" + popularity + ", revenue=" + revenue + ", fortuneListed=" + fortuneListed
		    + ", email=" + email + ", label=" + label + ", compId=" + compId + ", execId=" + execId + ", empId=" + empId + ", probability=" + probability + "]";
	}
}
