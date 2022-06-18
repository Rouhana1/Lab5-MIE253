package Lab5;

import java.io.*;
import java.nio.channels.FileChannel;
import java.sql.*;

public class Lab5 {
	
	public static void main(String[] args) throws IOException, SQLException {
		
		//Connect to DB
		//commentt
		Connection conn = null;
		try {
			conn = getConnection();
		} catch (SQLException e) {
			System.out.println("Error connecting to database.");
			e.printStackTrace();
		}
				
		//Restore the database to its initial state
		String sourcePath = ".\\assignments-lab5-before.sqlite";
		String destPath = ".\\assignments-lab5.sqlite";
		File source = new File(sourcePath);
		File dest = new File(destPath);
		restoreDBtoBefore(source, dest);
			
		//Execute transactions from the Input_Trans table
		executeAllTransactions(conn);

		//Close the DB connection
		try {
			conn.close();
		} catch (SQLException e) { //Catch if the connection is closed improperly
			System.out.println("Error disconnecting from database.");
			e.printStackTrace();
		}
		
	}
	
	//Method to establish database connection
	public static Connection getConnection() throws SQLException {
		//Form connection
		String DBName = "jdbc:sqlite:assignments-lab5.sqlite";
		Connection conn = DriverManager.getConnection(DBName);
		
		//Enable foreign keys
		Statement stmt = conn.createStatement();
		String enableFK = "PRAGMA foreign_keys = ON;";
		stmt.execute(enableFK);
		
		// SQL statements will be grouped into transactions and we have to explicitly end each transaction by using either commit or rollback.
		//IMPORTANT: The next line should be commented to answer a question.
		conn.setAutoCommit(false);
		
		return conn;
	}
	
	//This method will execute all the transactions by reading input parameters from the APAJSC_Trans_Input table 
	public static void executeAllTransactions(Connection conn) throws SQLException{
		try {
			Statement stmt = conn.createStatement(); // Code for creating statement
			String query = "SELECT * FROM APAJSCAUDR_Trans_Input;"; // Set query to be executed
			ResultSet rs = stmt.executeQuery(query); // Execute the query
			while (rs.next()) {
				//Read input parameters from Input_Trans table
				int transactionId = rs.getInt("TransactionID");
				int newProjectNumber = rs.getInt("NewProjectNumber");
				String newProjectName = rs.getString("NewProjectName");
				String newProjectDescription = rs.getString("NewProjectDescription");
				int newProjectWorkDays = rs.getInt("NewProjectWorkDays");
				int countSeniorContractors = rs.getInt("CountSeniorContractors");
				int countJuniorContractors = rs.getInt("CountJuniorContractors");
				String seniorYearHired = rs.getString("SeniorYearHired");
				String juniorYearHired = rs.getString("JuniorYearHired");
				int leadContractorNumber = rs.getInt("leadContractorNumber");
				int increaseAmount = rs.getInt("IncreaseAmount");
				
				//Print transactionId for transaction being executed
				System.out.println("Beginning transaction " + transactionId);
				
				//Initialize APAJSC and execute transaction
				AddProjectAndAssignJuniorAndSeniorContractorsAndUpdateDailyRate APAJSCAUDR = 
						new AddProjectAndAssignJuniorAndSeniorContractorsAndUpdateDailyRate(conn, transactionId, newProjectNumber, 
								newProjectName, newProjectDescription, newProjectWorkDays, 
								countSeniorContractors, countJuniorContractors, seniorYearHired, 
								juniorYearHired, leadContractorNumber, increaseAmount);
				//Use nested try-catch to catch errors on APAJSC.execute method
				try {
					APAJSCAUDR.execute();
				} catch (SQLException e) {
					System.out.println(e.getMessage());
					//System.out.println("Error executing transaction for transaction number " + transactionId + ".");
				}
			}
		} catch (SQLException e) {
			System.out.println("Error reading APAJSC_Trans_Input.");
			throw e;
		}
	}	
	
	//Method to restore the database to original state (before transaction)
	private static void restoreDBtoBefore(File source, File dest) throws IOException {
	    FileChannel sourceChannel = null;
	    FileChannel destChannel = null;
	    try {
	        sourceChannel = new FileInputStream(source).getChannel();
	        destChannel = new FileOutputStream(dest).getChannel();
	        destChannel.transferFrom(sourceChannel, 0, sourceChannel.size());
	       } finally {
	           sourceChannel.close();
	           destChannel.close();
	   }
	}
}
