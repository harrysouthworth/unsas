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

  public static void main(String [] args) {
    if (args.length != 2) {
      System.err.println("You need to enter precisely 2 command line arguments:\nthe input filename and the output filename");
      System.err.println("Number of args: " +args.length);
      System.exit(1);
    }
    SasParser parser = new SasParser();
    parser.createDataFile(args);
  }
  
  private SasParser() { }

  private void createDataFile(String [] args) {
    try {
      File file = new File(args[0]);
      FileInputStream fis = new FileInputStream(file);
      SasFileReader sasFileReader = new SasFileReader(fis);
      Writer writer = new FileWriter(args[1]);
      CSVDataWriter csvDataWriter = new CSVDataWriter(writer);
      csvDataWriter.writeColumnNames(sasFileReader.getColumns());
      csvDataWriter.writeRowsArray(sasFileReader.getColumns(), sasFileReader.readAll());
    } 
    catch (IOException ioe) {
      System.err.println("There has been an IO error");
      System.err.println("Details:...");
      ioe.printStackTrace();
    }
  } /* Close createDataFile */
} /* Close SasParser */
