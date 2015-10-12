package com.dcc.unsas.main;

import java.nio.charset.Charset;
import java.sql.*;
import java.awt.List;
import java.util.*;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.io.Writer;

import com.ggasoftware.parso.CSVDataWriter;
import com.ggasoftware.parso.CSVMetadataWriter;
import com.ggasoftware.parso.Column;
import com.ggasoftware.parso.SasFileReader;
//import com.ggasoftware.parso.StringWriter;


class unsas {
  public static void main(String[] args){
    if (args.length != 1){
      System.out.println("You need to specify a single argument - the path");
      System.exit(1);
    }

    /* Create csv directory */
    String csv = args[0] + "/csv";
    String meta = csv + "/meta/";
    String sqlite = args[0] + "/sqlite";
    new File(sqlite).mkdirs();
    //System.out.println(csv);

    Boolean success = (new File(csv)).mkdirs();
    if (!success){
      System.out.println("Failed to create csv directory");
      System.exit(1);
    }
    success = (new File(meta)).mkdirs();
    if (!success){
    	System.out.println("Failed to create metadata directory");
    	System.exit(1);
    }

    /* Get names of SAS files in the path */
    String[] fns = getSasFilenames(args[0]);

    /* Convert the files */
    for (String fn : fns){
      String inf = args[0] + "/" + fn + ".sas7bdat";
      String ouf = args[0] + "/csv/" + fn + ".csv";
      String db = args[0] + "/sqlite/" + "sqlite.db";
      String mdf = args[0] + "/csv/meta/" + fn + ".csv";

      // Create the CSV and SQLite. The CSV is mostly for debugging and is wasteful of disc space
      // DO NOT DELETE THE CSV CODE!!
      //System.out.println(fn + ".sas7bdat -> csv/" + fn + ".csv");
      //createDataFile(inf, ouf, mdf);
      createSQLiteDB(inf, db, fn);
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

  private static void createDataFile(String sasfile, String csvfile, String metafile) {
    try {
      File file = new File(sasfile);

      FileInputStream fis = new FileInputStream(file);
      SasFileReader sasFileReader = new SasFileReader(fis);

      // Write the data
      Writer writer = new FileWriter(csvfile);
      CSVDataWriter csvDataWriter = new CSVDataWriter(writer);
      csvDataWriter.writeColumnNames(sasFileReader.getColumns());
      csvDataWriter.writeRowsArray(sasFileReader.getColumns(), sasFileReader.readAll());
      
      // Write the metadata
      Writer mwriter = new FileWriter(metafile);
      CSVMetadataWriter csvMetadataWriter = new CSVMetadataWriter(mwriter);
      csvMetadataWriter.writeMetadata(sasFileReader.getColumns());
    }
    catch (IOException ioe) {
      System.err.println("There has been an IO error");
      System.err.println("Details:...");
      ioe.printStackTrace();
    }
  } /* Close createDataFile */

  private static void createSQLiteDB(String sasfile, String dbfile,String tbl) {
    Connection c = null;
    Statement stmt = null;

    String splitter = "\\.(?=[^\\.]+$)";

    try {
      File file = new File(sasfile);
      FileInputStream fis = new FileInputStream(file);
      SasFileReader sasFileReader = new SasFileReader(fis);

      Class.forName("org.sqlite.JDBC");
      c = DriverManager.getConnection("jdbc:sqlite:" + dbfile);
      System.out.println("Adding " + tbl + " to database");

      // Create table
      stmt = c.createStatement();
      String sql = "CREATE TABLE " + tbl + " (";

      /*************************************************************************
       ***************************** Column names ******************************
       *****************************              *****************************/
      int i=0;
      String type;
      ArrayList<String> cols = new ArrayList<String>();
      while (true){
        try { // Column names must be in quotes
          type = sasFileReader.getColumns().get(i).getType().getName();
          type = type.split(splitter)[1];
          if (type.equals("Number")){
            type = "REAL";
          }
          else if (type.equals("String")){
            type = "TEXT";
          }
          else {
            System.err.println("Field of type other than Number or String: " + type);
            System.exit(0);
          }
          cols.add(sasFileReader.getColumns().get(i).getName());
          sql += "\"" + cols.get(i) + "\" " + type + ",\n";
          i++;
        }
        catch(Exception e){
          // Remove the final comma
          sql = sql.substring(0, sql.length() - 2);
          break;
        }
      }
      sql += ")";
      stmt.executeUpdate(sql);

      /*************************************************************************
      ********************       Write the metadata      ***********************
      ********************                               **********************/
      
      // Look in csvMetadataWriter: column names are pretty much hardcoded
      sql = "CREATE TABLE " + tbl + "Meta (number INTEGER, name TEXT, type TEXT, dataLength INTEGER, format TEXT, label TEXT);";
      stmt.execute(sql);

      i = 0;
      stmt = c.createStatement();

      while(true){
        try{
          sql = "INSERT INTO " + tbl + " VALUES (";
          sql += sasFileReader.getColumns().get(i).getId() + ", ";
          sql += sasFileReader.getColumns().get(i).getName() + ", ";
          sql += sasFileReader.getColumns().get(i).getType().getName()
                 .replace("java.lang.Number", "Numeric")
                 .replace("java.lang.String", "Character") + ", ";
          sql += sasFileReader.getColumns().get(i).getLength() + ", ";
          sql += sasFileReader.getColumns().get(i).getFormat() + ", ";
          sql += sasFileReader.getColumns().get(i).getLabel() + ");";

          stmt.execute(sql);
          i ++;
        } // Close try
        catch (Exception e){
          break;
        }
      } // Close while

      /*************************************************************************
       ******************************* Add data ********************************
       *******************************          *******************************/

      try {
        Object [][] data = sasFileReader.readAll();

        int j;
        for (j=0; j < data.length; j++){
          sql = "";
          for(i=0; i < data[j].length; i++){
            if (data[j][i] == null) sql += "\"NULL\",";
            else{
              sql += "\"" + data[j][i].toString().replaceAll("[\\n\\r\\t]",  " ").replaceAll("\"",  "''") + "\",";
            }
          }
          sql = sql.substring(0, sql.length() - 1); // Remove last comma

          sql = "INSERT INTO " + tbl + " VALUES (" + sql + ");";
          stmt.execute(sql);
         } // Close for (j=0
      } // Close try
      catch(Exception e){
        System.err.println("Failed to write line to SQLite: " + sql);
        e.printStackTrace();
        System.exit(0);
      }

      /*************************************************************************
       ********************************* Tidy up ******************************* 
       *********************************         ******************************/

      stmt.close();
      c.close();
    } // Close try
    catch (IOException ioe) {
      System.err.println("There has been an IO error");
      System.err.println("Details:...");
      ioe.printStackTrace();
    } // Close catch
    catch (Exception e) {
        System.err.println( e.getClass().getName() + ": " + e.getMessage() );
        System.exit(0);
    }
  } // Close createSQLiteDB
  
  public static String tidySQL(String sql){
      // SQLite doesn't recognize ",," as being a null value or empty string.
      // We also need to deal with quotes and with commas in quoted strings.
      // A mature CSV parser would be better, but this appears to work for now
      int i;

      // The -1 argument in the next line tells split NOT to drop trailing empty fields
      //String [] ss = sql.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1); THIS DOESN'T WORK PROPERLY
      String [] ss = sql.split("\t", -1);
      sql = "";
      //System.out.println(ss.length);
      for (i=0; i < ss.length; i++){
        if (ss[i].length() == 0) ss[i] = "NULL";
        // If quoted, remove quotes
        else if (ss[i].substring(0) == "\"" & ss[i].substring(ss[i].length() -1) == "\"")
        	ss[i] = ss[i].substring(1, ss[i].length() -2);
        ss[i] = ss[i].replaceAll("\"",  "\'"); // Remove any remaining quotes
        //System.out.println(ss[i]);
        sql += "\"" + ss[i] + "\","; // Wrap in "" (SQLite will remove them for REALs
      }
      // Remove last comma
      sql = sql.substring(0, sql.length() - 1);
      return(sql);
  } // Close tidySQL
} // Close unsas
