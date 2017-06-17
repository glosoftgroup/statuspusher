package com.cellulant.statusPusher.utils;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.String;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import org.apache.commons.codec.binary.Hex;

/**
 * Loads system properties from a file.
 *
 * @author <a href="brian.ngure@cellulant.com">Brian Ngure</a>
 */
@SuppressWarnings({"FinalClass", "ClassWithoutLogger"})
public final class PushProps {

    /**
     * A list of any errors that occurred while loading the properties.
     */
    private transient List<String> loadErrors;
    /**
     * Info log level. Default = INFO.
     */
    private transient HashMap<String, HashMap> pushInformation;
    /**
     * The properties file.
     */
    private final String propsFile = "conf/PushUrls.xml";
    /**
     * The string to append before the string being logged.
     */
    private static String currentDigest = "";

    /**
     * Class Constructor.
     */
    public PushProps() {
        loadErrors = new ArrayList<String>(0);
        MessageDigest md = null;
        String digest = "";

        try {
            md = MessageDigest.getInstance("MD5");
            digest = getDigest(new FileInputStream(propsFile), md, 2048);
        } catch (IOException | NoSuchAlgorithmException ex) {
            System.err.println("Exiting. Error accessing properties file: "
                + ex.getMessage());
            System.exit(1);
        }

        loadProperties(propsFile);

        currentDigest = digest;
    }

    public String getDigest(InputStream is, MessageDigest md, int byteArraySize)
        throws NoSuchAlgorithmException, IOException {

        md.reset();
        byte[] bytes = new byte[byteArraySize];
        int numBytes;
        while ((numBytes = is.read(bytes)) != -1) {
            md.update(bytes, 0, numBytes);
        }
        byte[] digest = md.digest();
        String result = new String(Hex.encodeHex(digest));
        return result;
    }

    /**
     * Checks if there is any update on the file.
     *
     * @return TRUE if there is an update and FALSE if no update exists.
     */
    public boolean updateExists() {
        MessageDigest md = null;
        String digest = "";
        try {
            md = MessageDigest.getInstance("MD5");
            digest = getDigest(new FileInputStream(propsFile), md, 2048);
        } catch (IOException | NoSuchAlgorithmException ex) {
            System.err.println("Exiting. Error accessing properties file: "
                + ex.getMessage());
            System.exit(1);
        }

        if (!digest.equals(currentDigest)) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * Load system properties.
     *
     * @param propsFile the system properties xml file
     */
    @SuppressWarnings("UseOfSystemOutOrSystemErr")
    private void loadProperties(final String propsFile) {
        FileInputStream propsStream = null;
        Properties props;

        try {
            props = new Properties();
            propsStream = new FileInputStream(propsFile);
            props.loadFromXML(propsStream);

            String error1 = "ERROR: %s is may not have been set";

            Enumeration<Object> propertiesAvailable = props.keys();

            pushInformation = new HashMap<>(2);
            while (propertiesAvailable.hasMoreElements()) {
                String key = propertiesAvailable.nextElement().toString();

                String[] subkey = props.getProperty(key).split(",");

                HashMap<String, String> actualInfo = new HashMap<>(2);
                for (String id : subkey) {
                    String[] split = new String[2];

                    split = id.split("=", 2);

                    if (split[1].isEmpty()) {
                        split[1] = " ";
                    }

                    actualInfo.put(split[0], split[1]);
                }

                pushInformation.put(key, actualInfo);


            }
           
            propsStream.close();
        } catch (NumberFormatException ne) {
            System.err.println("Exiting. String value found, Integer is "
                + "required: " + ne.getMessage());

            try {
                propsStream.close();
            } catch (IOException ex) {
                System.err.println("Failed to close the properties file: "
                    + ex.getMessage());
            }
            System.exit(1);
        } catch (FileNotFoundException ne) {
            System.err.println("Exiting. Could not find the properties file: "
                + ne.getMessage());

            try {
                propsStream.close();
            } catch (IOException ex) {
                System.err.println("Failed to close the properties file: "
                    + ex.getMessage());
            }

            System.exit(1);
        } catch (IOException ioe) {
            System.err.println("Exiting. Failed to load system properties: "
                + ioe.getMessage());

            try {
                propsStream.close();
            } catch (IOException ex) {
                System.err.println("Failed to close the properties file");
            }

            System.exit(1);
        }
    }

    /**
     * A list of any errors that occurred while loading the properties.
     *
     * @return the loadErrors
     */
    public List<String> getLoadErrors() {
        return Collections.unmodifiableList(loadErrors);
    }

    /**
     * Information used during the push of status.
     *
     * @return the pushInformation
     */
    public HashMap<String, HashMap> getPushInformation() {
        return pushInformation;
    }
}