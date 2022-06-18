package Lab5;

public class Projects_E {
	private int projectNumber;
	private String projectName;
	private String projectDescription;

	public Projects_E(int projectNumber, String projectName, String projectDescription) {
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
