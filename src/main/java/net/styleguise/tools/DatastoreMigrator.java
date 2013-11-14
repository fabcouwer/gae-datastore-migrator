package net.styleguise.tools;

import java.io.Console;
import java.nio.file.Path;
import java.util.List;

public class DatastoreMigrator {

	/**
	 * Convenient utility for exporting from GAE datastore in the cloud into DevMode datastore on localhost.
	 */
	public static void main(String[] args) throws Exception {

		Console console = System.console();

		System.out.println("Enter your GAE credentials for administering styleguise-marketplace");
		String email = console.readLine("Email: ");
		String password = new String(console.readPassword("Password: "));

		System.out.println("Enter host and port for the remote datastore");
		String host = console.readLine("Host (ex. APPNAME.appspot.com): ");
		int port = Integer.parseInt(console.readLine("Port (on GAE 443, on localhost 8888): "));

		DatastoreExporter exporter = new DatastoreExporter(host, port, email, password);
		List<Path> dataFiles = exporter.exportData();
		exporter.close();

		DatastoreImporter importer = new DatastoreImporter(DatastoreImporter.Localhost, DatastoreImporter.DevRemoteApiPort, email, password);
		importer.importData(dataFiles);
		importer.close();
	}

}
