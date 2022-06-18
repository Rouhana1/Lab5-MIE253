package Lab5;

public class Assignment_R {
	private int projectNumber;
	private String projectName;
	private String projectDescription;
	
	public Assignment_R(int projectNumber, String projectName, String projectDescription) {
		this.projectNumber = projectNumber;
		this.projectName = projectName;
		this.projectDescription = projectDescription;
	}
	
	public int getProjectNumber() {
		return projectNumber;
	}
	
	public String getProjectName() {
		return projectName;
	}
	
	public String getProjectDescription() {
		return projectDescription;
	}
	
}
