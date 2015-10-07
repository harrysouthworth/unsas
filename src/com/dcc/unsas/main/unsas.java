package com.dcc.unsas.main;

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

    /*Boolean success = (new File(csv)).mkdirs();
    if (!success){
      System.out.println("Failed to create csv directory");
      System.exit(1);
    }
    success = (new File(meta)).mkdirs();
    if (!success){
    	System.out.println("Failed to create metadata directory");
    	System.exit(1);
    }*/

    /* Get names of SAS files in the path */
    String[] fns = getSasFilenames(args[0]);

    /* Convert the files */
    for (String fn : fns){
      String inf = args[0] + "/" + fn + ".sas7bdat";
      String ouf = args[0] + "/sqlite/" + "sqlite.db";
      String mdf = args[0] + "/csv/meta/" + fn + ".csv";

//      System.out.println(fn + ".sas7bdat -> csv/" + fn + ".csv");
      createSQLiteDB(inf, ouf, fn);
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

      // Create temporary file for writing rows to, then reading from
      File fn = File.createTempFile(".temp", null);
      Writer writer = new FileWriter(fn.getName());
      CSVDataWriter csvDataWriter = new CSVDataWriter(writer);

      // Now read the data
      FileReader fr = new FileReader(fn.getName());
      BufferedReader textReader = new BufferedReader(fr);

      while (true){
        try {
          // Get the values
          csvDataWriter.writeRow(sasFileReader.getColumns(), sasFileReader.readNext());

          String line;
          sql = "";
          while ((line = textReader.readLine()) != null) // Read everything, not just one line
            sql += line;
          sql = sql.replace("\n", " "); // Remove newlines

          if (sql == null | sql == "") break;

          sql = tidySQL(sql);
          sql += ");";

          sql = "INSERT INTO " + tbl + " VALUES (" + sql;
          stmt.execute(sql);
        }
        catch(Exception e){
          System.err.println("Failed to write line to SQLite: " + sql);
          break;
        }
      } // Close while

      /*************************************************************************
      ********************       Write the metadata      ***********************
      ********************                               **********************/

      File mfn = File.createTempFile(".temp", null);
      Writer mwriter = new FileWriter(mfn.getName());
      CSVMetadataWriter csvMetadataWriter = new CSVMetadataWriter(mwriter);
      csvMetadataWriter.writeMetadata(sasFileReader.getColumns());

      // Now read the data
      FileReader mfr = new FileReader(mfn.getName());
      BufferedReader mtextReader = new BufferedReader(mfr);

      stmt = c.createStatement();

      System.out.println("... and metadata");

      sql = "CREATE TABLE " + tbl + "Meta (";
      String newline = mtextReader.readLine(); // First line is colnames
      newline = newline.replace(" ", "_");
      sql += newline; 
      sql = sql.replace(",", " TEXT, ") + " TEXT";
      sql += ");";
      //System.out.println(sql);
      stmt.execute(sql);

      while((newline = mtextReader.readLine()) != null){
        //newline = "\"" + newline + "\"";
        newline = tidySQL(newline);
        sql = "INSERT INTO " + tbl + "Meta VALUES (" + newline + ")";
        //System.out.println(sql);
        stmt.execute(sql);
      }

      /*************************************************************************
       ********************************* Tidy up ******************************* 
       *********************************         ******************************/

      stmt.close();
      c.close();
      writer.close();
      mwriter.close();
      textReader.close();
      mtextReader.close();
      fr.close();
      // fn.delete() doesn't work, possibly because sasFileReader is still open and has no close method
      Runtime.getRuntime().exec("rm " + fn.getName());
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
      String [] ss = sql.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1);
      sql = "";
      //System.out.println(ss.length);
      for (i=0; i < ss.length; i++){
        if (ss[i].length() == 0) ss[i] = "NULL";
        // If quoted, remove quotes
        else if (ss[i].substring(0) == "\"" & ss[i].substring(ss[i].length() -1) == "\"")
        	ss[i] = ss[i].substring(1, ss[i].length() -2);
        ss[i] = ss[i].replace("\"",  "\'"); // Remove any remaining quotes
        sql += "\"" + ss[i] + "\","; // Wrap in "" (SQLite will remove them for REALs
      }
      // Remove last comma
      sql = sql.substring(0, sql.length() - 1);
      return(sql);
  } // Close tidySQL
} // Close unsas
