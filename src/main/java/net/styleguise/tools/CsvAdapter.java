package net.styleguise.tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;

public class CsvAdapter {

	private static String sourceDir = "D:\\Afstuderen\\Data Dumps\\";
	private static String targetDir = "D:\\Afstuderen\\Data Dumps\\edited\\";
	private static String acceptedIDsFile = "D:\\Afstuderen\\Data Dumps\\stored_ids.txt";

	private static ArrayList<String> ACCEPTED_IDS;

	public static void main(String[] args) {
		String filename = "HITgroup-6937866534143009333.csv";

		ACCEPTED_IDS = readAcceptedIDs();

		try {

			FileInputStream fis = new FileInputStream(sourceDir + filename);

			BufferedReader reader = new BufferedReader(new InputStreamReader(
					fis));
			String line = "";
			String[] nextLine;

			ArrayList<String[]> output = new ArrayList<String[]>();

			ArrayList<String> output2 = new ArrayList<String>();

			// Add first line
			line = reader.readLine();
			output.add(line.split(","));

			// Then read each line in turn
			String currentId = "";
			int amount = 0;
			while ((line = reader.readLine()) != null) {
				nextLine = line.split(",");
				amount++;
				// currentId = getCurrentIdInstance(nextLine[0]);
				currentId = getCurrentId(nextLine[0]);
				if (Long.parseLong(nextLine[1]) > 1438426660000L) {
					output.add(nextLine);
				}

			}

			reader.close();
			outputToCsv(output, filename);
			System.out.println("Looked at " + amount + " entries.");
			System.out.println("Cloned " + output.size() + " entries.");

			outputToCsvTemp(output2, "requestersAndHITs.csv");

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public static void outputToCsv(ArrayList<String[]> output, String fileName) {
		try {
			CSVWriter writer = new CSVWriter(new FileWriter(targetDir
					+ fileName), ',');
			String[] current = new String[1];
			for (String[] entries : output) {
				current[0] = "";
				for (String s : entries) {
					current[0] += addQuotes(s) + ",";
				}
				current[0] = current[0].substring(0, current[0].length() - 1);
				writer.writeNext(current);
			}
			writer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	//For outputting a list of Strings instead of list of String[]
	public static void outputToCsvTemp(ArrayList<String> output, String fileName) {
		try {
			CSVWriter writer = new CSVWriter(new FileWriter(targetDir
					+ fileName), ',');
			for (String entry : output) {
				writer.writeNext(new String[] { entry });
			}
			writer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static String addQuotes(String s) {
		// If it contains spaces or commas add quotes around it
		if (s.contains(" ") || s.contains(",")) {
			s = "\"" + s + "\"";
		}
		return s;
	}

	private static ArrayList<String> readAcceptedIDs() {
		ArrayList<String> result = new ArrayList<String>();
		File f = new File(acceptedIDsFile);
		try (BufferedReader br = new BufferedReader(new FileReader(f))) {
			String line;
			while ((line = br.readLine()) != null) {
				result.add(line);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	// Get actual ID from a String formatted as tableName("ID")
	private static String getCurrentId(String string) {
		int start = string.indexOf("(\"") + 2;
		int stop = string.indexOf("\")");
		return string.substring(start, stop);
	}

	private static String getCurrentIdInstance(String string) {
		int start = string.indexOf("(\"") + 2;
		int stop = string.indexOf("_");
		return string.substring(start, stop);
	}

}
