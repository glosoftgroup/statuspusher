package com.cellulant.statusPusher;

import com.cellulant.statusPusher.db.MySQL;
import com.cellulant.statusPusher.utils.Logging;
import com.cellulant.statusPusher.utils.Props;
import com.cellulant.statusPusher.utils.PushProps;
import java.sql.SQLException;
import java.util.Calendar;

/**
 * <p>Java UNIX daemon test file.</p> <p>Title: TestStatusPusher.java</p>
 * <p>Description: This class is used to test the functionality of the Java
 * Daemon.</p> <p>Created on 21 March 2012, 10:48</p> <hr />
 *
 * @since 1.0
 * @author <a href="brian.ngure@cellulant.com">Brian Ngure</a>
 * @version Version 1.0
 */
@SuppressWarnings({"ClassWithoutLogger", "FinalClass"})
public final class TestStatusPusher {

    /**
     * Logger for this application.
     */
    private static Logging log;
    /**
     * Loads system properties.
     */
    private static Props props;
    /**
     * Initializes the MySQL connection pool.
     */
    private static MySQL mysql;
    /**
     * The main run class.
     */
    private static StatusPusher statusPusher;
    /**
     * The string to append before the string being logged.
     */
    private static String logPreString = "TestBeepMessaging | ";
    /**
     * Push Urls Properties instance.
     */
    private static transient PushProps pushInfo;

    /**
     * Private constructor.
     */
    private TestStatusPusher() {
    }
    DaemonRun d = new DaemonRun();

    /**
     * Test init().
     */
    public static void init() {
        String logPreStringExt = logPreString + "init() | -1 | ";


        try {
            props = new Props();
            log = new Logging(props);
            pushInfo = new PushProps();
            mysql = new MySQL(props.getDbHost(), props.getDbPort(),
                props.getDbName(), props.getDbUserName(),
                props.getDbPassword(), props.getDbPoolName(), 20);
            //load a properties file

            log.info(logPreStringExt + "Loading the push configs for the first time...");

            statusPusher = new StatusPusher(props, pushInfo, log, mysql);
        } catch (ClassNotFoundException | InstantiationException |
            IllegalAccessException | SQLException ex) {
            log.fatal(logPreStringExt + "Exception caught during initialization" + ex);
            System.exit(1);
        }


    }

    /**
     * Main method.
     *
     * @param args command line arguments
     */
    @SuppressWarnings({"SleepWhileInLoop", "UseOfSystemOutOrSystemErr"})
    public static void main(final String[] args) {
        init();
        System.out.println("Please use /etc/init.d/statusPusher start|stop");


        String logPreStringExt = logPreString + "main() | -1 | ";

        //get current date time with Calendar()
        Calendar nextTime = Calendar.getInstance();
        nextTime.add(Calendar.SECOND, props.getRefreshUrlsInterval());
        while (true) {
            log.info("");
            try {
                Calendar now = Calendar.getInstance();

                if (now.compareTo(nextTime) >= 0) {
                    log.info(logPreStringExt + "Refreshing the push configs");
                    if (pushInfo.updateExists()) {
                        log.info(logPreStringExt + "An update exists in the Client"
                            + " Config file. Refreshing the File");
                        pushInfo = new PushProps();
                        statusPusher.setPushInfo(pushInfo);
                    }
                    nextTime = Calendar.getInstance();
                    nextTime.add(Calendar.SECOND, props.getRefreshUrlsInterval());
                }

                statusPusher.runDaemon();
            } catch (Exception ex) {
                log.fatal(logPreStringExt + "Error occured: " + ex);
            }

            try {
                Thread.sleep(1000);
            } catch (InterruptedException ex) {
                System.err.println("Error: " + ex);
            }
        }
    }
}