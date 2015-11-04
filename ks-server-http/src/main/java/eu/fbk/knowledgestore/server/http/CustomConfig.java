package eu.fbk.knowledgestore.server.http;

/**
 * Created by alessio on 05/10/15.
 */

public class CustomConfig {

	private String name, command;

	public CustomConfig(String name, String command) {
		this.name = name;
		this.command = command;
	}

	public String getName() {
		return name;
	}

	public String getCommand() {
		return command;
	}

	@Override
	public String toString() {
		return "CustomConfig{" +
				"name='" + name + '\'' +
				", command='" + command + '\'' +
				'}';
	}
}
