package Lab5;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


class AddProjectAndAssignJuniorAndSeniorContractorsAndUpdateDailyRate extends TransactionExecuter {
	
	//Initialize Input_Trans parameters
	int transactionId;
	int newProjectNumber;
	String newProjectName;
	String newProjectDescription;
	int newProjectWorkDays;
	int countSeniorContractors;
	int countJuniorContractors;
	String seniorYearHired;
	String juniorYearHired;
	int leadContractorNumber;
	int increaseAmount;


	//Only one constructor needed since all values must be used
	public AddProjectAndAssignJuniorAndSeniorContractorsAndUpdateDailyRate(Connection conn, int transactionId, int newProjectNumber, 
			String newProjectName, String newProjectDescription, int newProjectWorkDays, 
			int countSeniorContractors, int countJuniorContractors, String seniorYearHired,
			String juniorYearHired, int leadContractorNumber, int increaseAmount) {
		this.conn = conn;
		this.transactionId = transactionId;
		this.newProjectNumber = newProjectNumber;
		this.newProjectName = newProjectName;
		this.newProjectDescription = newProjectDescription;
		this.newProjectWorkDays = newProjectWorkDays;
		this.countSeniorContractors = countSeniorContractors;
		this.countJuniorContractors = countJuniorContractors;
		this.seniorYearHired = seniorYearHired;
		this.juniorYearHired = juniorYearHired;
		this.leadContractorNumber = leadContractorNumber;
		this.increaseAmount = increaseAmount;
	}
	
	public void outputFailure(int transactionID, String message) throws SQLException {
			
		//IMPORTANT: The next line should be commented to answer a question.
		conn.rollback();
		PreparedStatement ps = conn.prepareStatement("INSERT INTO Trans_Output (TransactionID, Message) values (?, ?)");
		ps.setInt(1, transactionID);
		ps.setString(2, message);
		ps.executeUpdate();	
		//IMPORTANT: The next line should be commented to answer a question.
		conn.commit();
 

}
	
	public void outputSuccess(int transactionID, String message) throws SQLException {
		
		//IMPORTANT: The next line should be commented to answer a question.
		conn.commit();
		PreparedStatement ps = conn.prepareStatement("INSERT INTO Trans_Output (TransactionID, Message) values (?, ?)");
		ps.setInt(1, transactionID);
		ps.setString(2, message);
		ps.executeUpdate();		
		//IMPORTANT: The next line should be commented to answer a question.
		conn.commit();
 
}
	
	//Execute the transactions for each tuple in APAJSC_Trans
	public void execute() throws SQLException {	
		
		/* Transaction specification
		    Insert a tuple in Project_E with newProjectNumber, projectName, projectDescription
			Find in Contractors_E exactly countSeniorContractors suitable senior contractors, those hired before senior year hired
			Find in Contractors_E exactly countJuniorContractors suitable junior contractors, those hired after junior year hired
			Insert tuples into Assignment_R to assign to newProjectNumber the leadContractorNumber, and all the senior and junior contractors in the proper roles
			Each senior contractor works 1/2 the newProjectWorkDay
			Each junior contractor works newProjectWorkDay
			Each lead contractor works countSeniorContractors + countJuniorContractors
			Bonus is left null
			
			
		   Dynamic update constraints
		    Input_trans table adds a fixed increaseAmount (to increase DailyRate by fixed amount for all the senior contractors added to the project)
		    This requires an update on Contractor_E after the insert into Assignments_R. DynamicUpdate() function the the end does this update. 
		    A dynamic constraint to this update, "no contractor DailyRate increases by more than 20%" and enforce it on the update above 
		    The dynamic constraint matches through the selected senior contractors that should be added to the new project and checks if the 
		    oldDailyRate + fixedValue update goes over 20% oldDailyRate. If not, update is done. If yes, an error arises and the DailyRate
		    does not change. Finally, the contractorNumber and the related error message for all the failed cases are inserted into the table
		    OutputInsert. 
		*/ 
				   
		
		//Insert a tuple in Projects_E
		insertNewProject(newProjectNumber, newProjectName, newProjectDescription);
		
		//Select senior contractors from Contractors_E as those hired before seniorYearHired
		List<Contractors_E> seniorContractors = selectSeniorContractors(countSeniorContractors, 
				seniorYearHired);
		
		//Select junior contractors from Contractors_E as those hired after juniorYearHired
		List<Contractors_E> juniorContractors = selectJuniorContractors(countJuniorContractors, 
				juniorYearHired);
		
		//Insert tuples into Assignment_R for all senior contractors assigned to the project
		int seniorWorkDays = newProjectWorkDays / 2;	//Calculate number of work days for seniors assigned to the project
		
		Iterator<Contractors_E> seniorIterator = seniorContractors.iterator();		//Create iterator to go through list
		while (seniorIterator.hasNext()) {
			Contractors_E seniorContractor = (Contractors_E) seniorIterator.next();		//Go to next contractor in list
			insertAssignment(seniorContractor.getNumber(), newProjectNumber, seniorWorkDays, "Senior");	//Assign contractor to project
		}
		
		//Insert tuples into Assignment_R for all junior contractors assigned to the project
		int juniorWorkDays = newProjectWorkDays; 	//Calculate number of work days for juniors assigned to the project
		
		Iterator<Contractors_E> juniorIterator = juniorContractors.iterator();		//Create iterator to go through list
		while (juniorIterator.hasNext()) {
			Contractors_E juniorContractor = (Contractors_E) juniorIterator.next();		//Go to next contractor in list			
			insertAssignment(juniorContractor.getNumber(), newProjectNumber, juniorWorkDays, "Junior");	//Assign contractor to project
		}
			
		//Insert tuple into Assignment_R for lead contractor assigned to the project
		int leadWorkDays = seniorWorkDays + juniorWorkDays;		//Calculate number of work days for project leads
		insertAssignment(leadContractorNumber, newProjectNumber, leadWorkDays, "Lead");	//Assign contractor to project
		
		// Enforce the dynamic constraint when updating to increase the DailyRate of the senior contractors added to the new project
		dynamicConstraintUpdate(seniorContractors); 
		
		// All the changes have been made in the database and we are committing them
		
		String outMsg = "Transaction succeeded.";
		outputSuccess(transactionId, outMsg);
		System.out.println(outMsg); 
		conn.commit();
	}

	
	
	
	//Method to insert a new project in Projects_E
	private void insertNewProject(int newProjectNumber, String newProjectName, String newProjectDescription) throws SQLException {
		try {
			Statement stmt = conn.createStatement();
			String query = "INSERT INTO Projects_E (ProjectNumber, ProjectName, ProjectDescription) values (?, ?, ?);";	//Set query to execute
			PreparedStatement pstmt = conn.prepareStatement(query);			
			
			pstmt.setInt(1, newProjectNumber);
			pstmt.setString(2, newProjectName);
			pstmt.setString(3, newProjectDescription);
			pstmt.execute();				//Execute the query
			stmt.close();
			
		} catch (SQLException e) {
			if (e.getMessage().contentEquals("[SQLITE_CONSTRAINT]  Abort due to constraint violation (UNIQUE constraint failed: Projects_E.ProjectNumber)")) {
				String outMsg = "PK violation. Unable to insert project " + newProjectNumber + " into Project_E since the project number already exists in the table.";
				outputFailure(transactionId, outMsg);
				System.out.println(outMsg);
			    throw e;
			} else {
				String outMsg = "Unknown Exception.";
				outputFailure(transactionId, outMsg);
				throw e;
			}
		}
	}
	
	
	
	//Method to select senior level contractors to be assigned to new project
	private List<Contractors_E> selectSeniorContractors(int countSeniorContractors, 
			String seniorYearHired) throws SQLException {
		List<Contractors_E> seniorContractors = new ArrayList<>();	//Create list of senior contractors
		
		try {
			String query = "SELECT ContractorNumber,ContractorName,DailyRate,YearHired FROM Contractors_E WHERE YearHired < ?;"; // Set query to execute
			PreparedStatement pstmt = conn.prepareStatement(query);
			pstmt.setString(1, seniorYearHired);			
			ResultSet rs = pstmt.executeQuery(); 						// Execute the query
			for (int i = 0; i < countSeniorContractors; i++) {
				rs.next();									//Go to next result from query
				Contractors_E contractor = new Contractors_E(rs.getInt("ContractorNumber"), rs.getString("ContractorName"), rs.getInt("DailyRate"), rs.getDate("YearHired"));	//Create new contractor with contractorNumber
				seniorContractors.add(contractor);		//Add contractorNumber to list of senior contractors
			}
		} catch (SQLException e) {
			if (seniorContractors.size() < countSeniorContractors) {	//Check if required number of contractors can be selected
				String outMsg = "Not enough senior contractors.";
				outputFailure(transactionId, outMsg);
				System.out.println(outMsg);
				throw e;
			} else {
				String outMsg = "Unknown Exception.";
				outputFailure(transactionId, outMsg);
				throw e;
			}
		}
		
		return seniorContractors;	//Return list of senior contractors
	}
	
	//Method to choose junior level contractors to be assigned to new project
	private List<Contractors_E> selectJuniorContractors(int countJuniorContractors, String juniorYearHired) throws SQLException {
		List<Contractors_E> juniorContractors = new ArrayList<>();	//Create list of junior contractors
		
		try {
			String query = "SELECT ContractorNumber FROM Contractors_E WHERE YearHired > ?;"; // Set query to execute
			PreparedStatement pstmt = conn.prepareStatement(query);
			pstmt.setString(1, juniorYearHired);			
			ResultSet rs = pstmt.executeQuery(); 						// Execute the query
			for (int i = 0; i < countJuniorContractors; i++) {
				rs.next();							//Go to next result from query
				Contractors_E contractor = new Contractors_E(rs.getInt("ContractorNumber"));	//Create new contractor with contractorNumber
				juniorContractors.add(contractor);	//Add contractorNumber to list of junior contractors
			}
		} catch (SQLException e) {
			if (juniorContractors.size() < countJuniorContractors) {	//Check if required number of contractors can be selected
				String outMsg = "Not enough junior contractors.";
				outputFailure(transactionId, outMsg);
				System.out.println(outMsg);
				throw e;
			} else {
				String outMsg = "Unknown Exception.";
				outputFailure(transactionId, outMsg);
				throw e;
			}
		}
			
		return juniorContractors;	//Return list of junior contractors
	}
	
	//Method to insert new tuple into Assignment_R
	private void insertAssignment(int contractorNumber, int newProjectNumber, int workDays, String position) throws SQLException {
				
		try {
			Statement stmt = conn.createStatement();
			String query = "INSERT INTO Assignment_R (ContractorNumber, ProjectNumber, WorkDays, Position) values (?, ?, ?, ?);";	//Set query to execute
			PreparedStatement pstmt = conn.prepareStatement(query);			
			
			pstmt.setInt(1, contractorNumber);
			pstmt.setInt(2, newProjectNumber);
			pstmt.setInt(3, workDays);
			pstmt.setString(4, position);
			pstmt.execute();				//Execute the query
			stmt.close();
		
		} catch (SQLException e) {
			if (e.getMessage().contentEquals("[SQLITE_CONSTRAINT]  Abort due to constraint violation (FOREIGN KEY constraint failed)")) {
				String outMsg = "FK Violation. Contractor " + contractorNumber + " does not exist in Contractors_E.";
				outputFailure(transactionId, outMsg);
				System.out.println(outMsg);
				throw e;

			} else if (e.getMessage().contentEquals("[SQLITE_CONSTRAINT]  Abort due to constraint violation (UNIQUE constraint failed: Assignment_R.ContractorNumber, Assignment_R.ProjectNumber)")) {
				String outMsg = "PK Violation. An assignment already exists for contractor " + contractorNumber +
						" for project " + newProjectNumber + ".";
				outputFailure(transactionId, outMsg);
				System.out.println(outMsg);
				throw e;

			} else {
				String outMsg = "Unknown Exception.";
				outputFailure(transactionId, outMsg);
				throw e;
			}
		}
	}
	
	
		// Dynamic Constraint Update to increase the the DailyRate for the selected senoirContractors with a fixed increases amount in input transaction table 
		private void dynamicConstraintUpdate(List<Contractors_E> seniorContractors) throws SQLException {
			
			Iterator<Contractors_E> seniorIterator = seniorContractors.iterator();		//Create iterator to go through list
			while (seniorIterator.hasNext()) {
				Contractors_E seniorContractor = (Contractors_E) seniorIterator.next();		//Go to next contractor in list
				int newDailyRate = (int) (seniorContractor.getDailyRate() + increaseAmount);
				
				// Dynamic constraint enforcing

				// setup a flag which is used to check whether any errors were encountered
				// during the dynamic constrain checking
				boolean errorFlag = false;

				////
				// check that the new DailyRate is less increased less than 20% old DailyRate for each selected senior contractor
				double tolerance = 0.20d;
				double paymentUpperLimitDouble = seniorContractor.getDailyRate() * (1 + tolerance);

				// cast the double values to an integer
				int paymentUpperLimit = (int) paymentUpperLimitDouble;

				// check the range
				if (newDailyRate >= paymentUpperLimit) {
					// set the error flag to indicate that the upper limit is violated
					errorFlag = true;
				} 
								
				if (!errorFlag) 
				{
					
					
					System.out.println("Updating DailyRate for the senior contrator with contractor number: "
							+ seniorContractor.getNumber() + " from the old value: " + seniorContractor.getDailyRate() 
							+ " to the new value: "+ newDailyRate + " is DONE.");
					
					try {
						Statement stmt = conn.createStatement();
						String query = "UPDATE Contractors_E SET DailyRate=? WHERE ContractorNumber = ?;";	//Set query to execute
						PreparedStatement pstmt = conn.prepareStatement(query);			
						
						pstmt.setInt(1, newDailyRate);
						pstmt.setInt(2, seniorContractor.getNumber());
						pstmt.execute();				//Execute the query
						stmt.close();
						
						
					} catch (SQLException e) {
							throw e;			
					}
					
					
				} else
				{
					// inserting the messages that indicates errors during enforcing dynamic constrain into the OutputInsert table
					// along with the contractor number and printing reason of failure for each failed case			
					String outMsg = "Updating DailyRate for the senior contrator with contractor number: "
							+ seniorContractor.getNumber() + " from the old value: " + seniorContractor.getDailyRate() 
							+ " to the new value: "+ newDailyRate + " is FAILED due to violating the upper limit.";
					outputFailure(transactionId, outMsg);
					System.out.println(outMsg);
					throw new SQLException("Dynamic Constraint Violation.");						
					}		
					
				}

			}
				
	
}
