package com.cellulant.statusPusher.utils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * Utilities class.
 *
 * @author Brian Ngure
 */
@SuppressWarnings("FinalClass")
public final class Utilities {


    /**
     * Prepares the statement to store in the file.
     */
    public static String prepareSqlString(String query, String[] params, int index) {
        if (!query.contains("?")) {
            return query;
        }
        String s = query.replaceFirst("\\?", params[index]);
        return prepareSqlString(s, params, ++index);
    }

    /**
     * Store queries in a file NOTE: ----- Checks whether the queries file
     * exists and writes to it the queries that need to be performed
     *
     * Performance Issues ------------------- This has an effect on the speed of
     * execution-> speed reduces because of waiting for a lock to be unlocked,
     * the wait time is unlimited.
     *
     * @param file file and path to write to
     * @param data SQL string to write
     *
     * @return True if update was don and False if not
     */
    public static boolean updateFile(String file, String data) {
        boolean state = false;
        if (!fileExists(file)) {
            createFile(file);
            state = writeToFile(file, data);
        } else {
            state = writeToFile(file, data);
        }

        return state;
    }

    /**/
    /*--FILE MANIPULATION FUNCTIONS-- */
    /*
     Make a file
     NOTE: Must be used where we have read, write, update, delete access...
     */
    private static boolean createFile(String file) {
        boolean state = false;
        FileOutputStream fout = null;
        try {
            fout = new FileOutputStream(file);
            state = true;
        } catch (Exception e) {
            state = false;
        } finally {
            try {
                fout.close();
            } catch (IOException ex) {
                fout = null;
            }
        }
        return state;
    }

    /*check for file*/
    private static boolean fileExists(String file) {
        boolean state = false;
        try {
            File myfile = new File(file);
            if (myfile.exists()) {
                state = true;
            } else {
                state = false;
            }

        } catch (Exception e) {
            state = false;
        }
        return state;
    }

    /*Add data to a file
     NOTE:
     -----
     <TOCHECK> This function appends the queries file...
     */
    public static boolean writeToFile(String filepath, String data) {
        PrintWriter pout = null;
        boolean state = false;
        try {
            state = false;
            pout = new PrintWriter(new FileOutputStream(filepath, true));
            pout.println(data);

            pout.close();
            pout = null;
        } catch (Exception e) {
            state = false;
        } finally {
            if (pout != null) {
                pout.close();
            }
            pout = null;
        }

        return state;
    }

    public static boolean checkInArray(int currentState, int[] myArray) {
        for (int i : myArray) {

            if (i == currentState) {
                return true;
            }
        }
        return false;
    }

    /**
     * private constructor.
     */
    private Utilities() {
        // Empty
    }
}