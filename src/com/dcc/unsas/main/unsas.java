package com.dcc.unsas.main;

//import java.awt.List;
//import java.util.*;

//import java.io.BufferedInputStream;
//import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
//import java.io.InputStreamReader;
import java.io.Writer;

import com.ggasoftware.parso.CSVDataWriter;
//import com.ggasoftware.parso.CSVMetadataWriter;
import com.ggasoftware.parso.SasFileReader;


class unsas {
  public static void main(String[] args){
    if (args.length != 1){
      System.out.println("You need to specify a single argument - the path");
      System.exit(1);
    }

    /* Create csv directory */
    String csv = args[0] + "/csv";
    System.out.println(csv);
    Boolean success = (new File(csv)).mkdir();
    if (!success){
      System.out.println("Failed to create csv directory");
      System.exit(1);
    }
    /* Get names of SAS files in the path */
    String[] fns = getSasFilenames(args[0]);
    for (int i=0; i < fns.length; i++)
      System.out.println(fns[i] + ".sas7bdat -> csv/" + fns[i] + ".csv");

    /* Convert the files */
    for (String fn : fns){
      String inf = args[0] + "/" + fn + ".sas7bdat";
      String ouf = args[0] + "/csv/" + fn + ".csv";
      
      createDataFile(inf, ouf);
    }
  }

  private static String[] getSasFilenames(String path){
    File dir = new File(path);
    File[] files = dir.listFiles();
    int Nfiles=0;

    String splitter = "\\.(?=[^\\.]+$)";
    
    /* Count the SAS files in the path. Rubbish code starts here... */
    for (int i=0; i < files.length; i++){
      if (files[i].isFile()){
        String fl = files[i].getName();
        String[] sfl = fl.split(splitter);
        if (sfl[1].toLowerCase().trim().equals("sas7bdat"))
          Nfiles = Nfiles + 1;
      }
    }

    /* Create string array with correct length */
    String[] sasfiles = new String[Nfiles];

    int count=0;
    for (int i=0; i < files.length; i++){
      if (files[i].isFile()){
        String fl = files[i].getName();
        String[] sfl = fl.split(splitter);

        if (sfl[1].toLowerCase().trim().equals("sas7bdat")){
          sasfiles[count] = sfl[0];
          count++;
        }
      }
    }
  return(sasfiles);
  } /* Close getSasFilenames */

  private static void createDataFile(String sasfile, String csvfile) {
    try {
      File file = new File(sasfile);
      FileInputStream fis = new FileInputStream(file);
      SasFileReader sasFileReader = new SasFileReader(fis);
      Writer writer = new FileWriter(csvfile);
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
} /* Close First */
