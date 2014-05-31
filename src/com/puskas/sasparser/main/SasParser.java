package com.puskas.sasparser.main;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Writer;

import com.ggasoftware.parso.CSVDataWriter;
import com.ggasoftware.parso.CSVMetadataWriter;
import com.ggasoftware.parso.SasFileReader;

public class SasParser {

	public static void main(String [] args)
	{
		SasParser parser = new SasParser();
		parser.consoleOptions();
	}
	
	private SasParser()
	{

	}
	
	private void consoleOptions() {
		while (true) {
			System.out
					.println("Select option:\n 1. Convert metadata of a 7BDAT file to csv format \n 2. Convert data of a 7BDAT file to csv format \n 3. Display help \n 4. Exit");
			try {
				BufferedReader buffy = new BufferedReader(
						new InputStreamReader(System.in));
				int i = Integer.parseInt(buffy.readLine());
				switch (i) {
				case 1:
					createMetaDataFile(buffy);
					break;
				case 2:
					createDataFile(buffy);
					break;
				case 3:
					displayHelp();
					break;
				case 4:
					System.out.println("OK - exiting");
					buffy.close();
					System.exit(0);
				default:
					System.err
							.println("You have 4 options. Just 1, 2, 3 or 4. Pick one of them.");
					break;
				}
			} catch (IOException ioe) {
				System.err.println("Couldn't read the input - ");
				ioe.printStackTrace();
			} catch (NumberFormatException nfe) {
				System.err
						.println("Please enter a number...");
			}
		}
	}
	
	private void createMetaDataFile(BufferedReader buffy)
	{
		try {
		System.out.println("Enter full path of SAS file to read");
	
		String fileName = buffy.readLine();
		System.out.println("And enter full path of output CSV file to create");
		String outputFile = buffy.readLine();
		File file = new File(fileName);
		FileInputStream fis = new FileInputStream(file);
		SasFileReader sasFileReader = new SasFileReader(fis);
		Writer writer = new FileWriter(outputFile);
		CSVMetadataWriter csvMetaDataWriter = new CSVMetadataWriter(writer);
		csvMetaDataWriter.writeMetadata(sasFileReader.getColumns());
		} 
		catch (IOException ioe)
		{
			System.err.println("There has been an IO error");
			System.err.println("Details:...");
			ioe.printStackTrace();
		}

	}
	
	private void createDataFile(BufferedReader buffy)
	{
		try {
			System.out.println("Enter full path of SAS file to read");
		
			String fileName = buffy.readLine();
			System.out.println("And enter full path of output CSV file to create");
			String outputFile = buffy.readLine();
			File file = new File(fileName);
			FileInputStream fis = new FileInputStream(file);
			SasFileReader sasFileReader = new SasFileReader(fis);
			Writer writer = new FileWriter(outputFile);
			CSVDataWriter csvDataWriter = new CSVDataWriter(writer);
			csvDataWriter.writeColumnNames(sasFileReader.getColumns());
			csvDataWriter.writeRowsArray(sasFileReader.getColumns(), sasFileReader.readAll());
			} 
			catch (IOException ioe)
			{
				System.err.println("There has been an IO error");
				System.err.println("Details:...");
				ioe.printStackTrace();
			}
	}
	
	private void displayHelp()
	{
		System.out.println("Enter 1,2,3 or 4, depending on what you want to do, then follow the instructions.");
	}
}
