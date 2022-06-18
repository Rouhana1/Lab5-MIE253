package Lab5;

import java.sql.Date;

public class Contractors_E {
	private int contractorNumber;
	private String contractorName;
	private int dailyRate;
	private Date yearHired;

	//We are defining this additional constructor, but it is not used for now
	public Contractors_E(int contractorNumber, String contractorName, int dailyRate, Date yearHired) {
		this.contractorNumber = contractorNumber;
		this.contractorName = contractorName;
		this.dailyRate = dailyRate;
		this.yearHired = yearHired;
	}
	
	//We use this constructor instead since we only need the contractor number
	public Contractors_E(int contractorNumber) {
		this.contractorNumber = contractorNumber;
	}

	public int getNumber() {
		return contractorNumber;
	}

	public String getName() {
		return contractorName;
	}

	public int getDailyRate() {
		return dailyRate;
	}

	public Date getYearHired() {
		return yearHired;
	}

}
