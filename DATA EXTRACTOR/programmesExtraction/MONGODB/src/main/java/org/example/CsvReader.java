package org.example;

import java.io.*;
import java.util.ArrayList;
import java.util.Scanner;

public class CsvReader {
    public static ArrayList<String[]> getCSV(String pathfile, boolean skipHeader) throws FileNotFoundException, UnsupportedEncodingException {

        FileInputStream is = new FileInputStream(new File(pathfile));
        InputStreamReader isr = new InputStreamReader(is, "WINDOWS-1252");
        Scanner sc = new Scanner(isr);
        sc.useDelimiter("\\n");   //sets the delimiter pattern


        ArrayList<String[]> arraytmp = new ArrayList<String[]>();

        if(skipHeader) {
            sc.next();
        }

        while (sc.hasNext())  //returns a boolean value
        {

            String line = sc.next();
            String[] lineArray = line.split(",");
            arraytmp.add(lineArray);
        }
        sc.close();
        return  arraytmp;
    }

    public static ArrayList<String[]> getCSV(String pathfile) throws FileNotFoundException, UnsupportedEncodingException {
        return getCSV(pathfile,true);
    }

}
