package com.cellulant.statusPusher;

import com.cellulant.statusPusher.db.MySQL;
import com.cellulant.statusPusher.utils.StatusPusherConstants;
import com.cellulant.statusPusher.utils.Logging;
import com.cellulant.statusPusher.utils.Props;
import com.cellulant.statusPusher.utils.PushProps;
import com.cellulant.statusPusher.utils.Utilities;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * StatusPusher worker class.
 *
 * @author Brian Ngure
 */
@SuppressWarnings("FinalClass")
public final class StatusPusher {

    /**
     * The MySQL data source.
     */
    private transient MySQL mysql;
    /**
     * File input stream to check for failed queries.
     */
    private FileInputStream fin;
    /**
     * Data input stream to check for failed queries.
     */
    private DataInputStream in;
    /**
     * Buffered reader to check for failed queries.
     */
    private BufferedReader br;
    /**
     * The daemons current state.
     */
    private transient int daemonState;
    /**
     * System properties class instance.
     */
    private transient Props props;
    /**
     * Log class instance.
     */
    private transient Logging logging;
    /**
     * The print out to external .TXT file
     */
    private transient PrintWriter pout;
    /**
     * Flag to check if current pool is completed.
     */
    private transient boolean isCurrentPoolShutDown = false;
    /**
     * The string to append before the string being logged.
     */
    private String logPreString;
    /**
     * Push Urls Properties instance.
     */
    private PushProps pushInfo;

    /**
     * Constructor. Checks for any errors while loading system properties,
     * creates the thread pool and resets partially processed records.
     *
     * @param log the logger class used to log information and errors
     * @param properties the loaded system properties
     */
    public StatusPusher(final Props properties, final PushProps pushInfo,
            final Logging log, final MySQL mySQL) {
        this.props = properties;
        this.logging = log;
        this.mysql = mySQL;
        this.pushInfo = pushInfo;

        this.logPreString = "PushPaymentStatus | ";
        String logPreString = this.logPreString + "PushPaymentStatus() | -1 | ";
        // Get the list of errors found when loading system properties
        List<String> loadErrors = properties.getLoadErrors();
        int sz = loadErrors.size();

        if (sz > 0) {
            log.info(logPreString + "There were exactly "
                    + sz + " error(s) during the load operation...");

            for (String err : loadErrors) {
                log.fatal(logPreString + err);
            }

            log.info(logPreString + "Unable to start daemon "
                    + "because " + sz + " error(s) occured during load.");
            System.exit(1);
        } else {
            log.info(logPreString
                    + "All required properties were loaded successfully");
            daemonState = StatusPusherConstants.DAEMON_RUNNING;
        }
    }

    /**
     * Method <i>getTasks</i> gets a bucket of unprocessed tasks and processes
     * them.
     */
    private synchronized void executeTasks() {
        PreparedStatement stmt = null;
        ResultSet rs = null;
        Connection conn = null;
        ExecutorService executor = null;
        String logPreString = this.logPreString + "executeTasks() | -1 | ";

        String query = "";

        int configuredClients = 0;
        String[] allClients = null;
        HashMap<String, HashMap> allData = null;
        try {

            allData = pushInfo.getPushInformation();

            if (allData == null) {
                logging.error(logPreString
                        + "No push information for clients found");
                return;
            }
            configuredClients = allData.size();
            allClients = allData.keySet().toArray(new String[configuredClients]);
            if (allClients == null) {
                logging.info(logPreString
                        + "No client configurations found.");
                return;
            }

        } catch (Exception e) {
            logging.error(logPreString + "Exception occured. Error "
                    + "Message :" + e);
        }
        for (int count = 0; count < configuredClients; count++) {
            try {
                String clientCode = Arrays.asList(allClients).get(count);
                HashMap clientdata = allData
                        .get(allClients[count]);

                if (clientdata == null) {
                    logging.error(logPreString
                            + "No configurations for client with client code: "
                            + clientCode + " found. ");
                    continue;
                }

                logging.debug(logPreString
                        + "Extracting data obtained from file: "
                        + clientdata.toString());

                String url = clientdata.get("URL").toString();
                String username = clientdata.get("USERNAME").toString();
                String password = clientdata.get("PASSWORD").toString();
                String protocol = clientdata.get("PROTOCOL").toString();
                String method = clientdata.get("METHOD").toString();
                int sslEnabled
                        = Integer.parseInt(clientdata.get("SSLENABLED")
                                .toString());
                String sslCertificatePath = "";
                if (sslEnabled == 1) {
                    sslCertificatePath = clientdata.get("SSLCERTPATH")
                            .toString();
                }

                logging.info(logPreString
                        + "Fetching records to be processed for client with "
                        + "client code " + clientCode + " ... ");

                query = "SELECT p.payerTransactionID, p.requestLogID, "
                        + "p.paymentAckDate, p.statusFirstSend, rl.serviceID, "
                        + "p.statusLastSend, p.statusNextSend, "
                        + "p.receiptNumber, p.receiverNarration, "
                        + "rl.overallStatus,cl.clientCode FROM "
                        + "s_payments p INNER JOIN s_requestLogs rl ON "
                        + "p.requestLogID = rl.requestLogID LEFT JOIN clients "
                        + "cl ON p.payerClientID = cl.clientID WHERE "
                        + "(statusPushed = ? OR statusPushed IS NULL)"
                        + "AND rl.overallStatus IN (" + props.getStatusToPush()
                        + ") AND cl.clientCode = ? AND (p.statusNextSend >= "
                        + "NOW() OR p.statusNextSend IS NULL) "
                        + "AND (p.statusFirstSend >= DATE_SUB(NOW(), INTERVAL "
                        + "? SECOND) OR p.statusFirstSend IS NULL) ORDER BY "
                        + "p.requestLogID ASC LIMIT ?;";

                conn = mysql.getConnection();

                stmt = conn.prepareStatement(query);
                stmt.setInt(1, props.getUnprocessedStatus());
                stmt.setString(2, clientCode);
                stmt.setInt(3, props.getPushAckTimeoutPeriod());
                stmt.setInt(4, props.getBucketSize());

                String[] params = {
                    String.valueOf(props.getUnprocessedStatus()),
                    String.valueOf(clientCode),
                    String.valueOf(props.getPushAckTimeoutPeriod()),
                    String.valueOf(props.getBucketSize())
                };
                rs = stmt.executeQuery();

                int size = 0;
                if (rs.last()) {
                    size = rs.getRow();
                    rs.beforeFirst();
                }
                if (size > 0) {
                    logging.info(logPreString
                            + "Fetch records using query = "
                            + Utilities.prepareSqlString(query, params, 0));
                    isCurrentPoolShutDown = false;

                    if (size <= props.getNumOfChildren()) {
                        executor = Executors
                                .newFixedThreadPool(size);
                    } else {
                        executor = Executors
                                .newFixedThreadPool(props.getNumOfChildren());
                    }

                    while (rs.next()) {
                        String payerTransactionID
                                = rs.getString("payerTransactionID");
                        int requestLogID = rs.getInt("requestLogID");
                        int overallStatus = rs.getInt("overallStatus");
                        int serviceID = rs.getInt("serviceID");
                        String receiptNumber = rs.getString("receiptNumber");
                        String receiverNarration = rs.getString("receiverNarration");
                        String nextSend = rs.getString("statusNextSend");
                        String firstSend = rs.getString("statusFirstSend");
                        String lastSend = rs.getString("statusLastSend");
                        // Create a runnable task and submit it

                        Runnable task = createTask(payerTransactionID,
                                requestLogID, overallStatus, serviceID,
                                receiptNumber, receiverNarration, clientCode,
                                nextSend, firstSend, lastSend, url, protocol,
                                sslEnabled, sslCertificatePath, username,
                                password, method);

                        executor.execute(task);
                    }
                    rs.close();
                    rs = null;
                    stmt.close();
                    stmt = null;
                    conn.close();
                    conn = null;
                    /*
                     * This will make the executor accept no new threads and
                     * finish all existing threads in the queue.
                     */
                    shutdownAndAwaitTermination(executor);
                } else {
                    logging.info(logPreString
                            + "No records were fetched from the DB for "
                            + "processing...");
                }

            } catch (SQLException e) {
                logging.error(logPreString + "Failed to "
                        + "fetch Bucket: Select Query: " + query + " Error "
                        + "Message :" + e.getMessage());
                continue;
            } catch (Exception e) {
                logging.error(logPreString + "Execption occured. Error "
                        + "Message :" + e);
                continue;
            } finally {
                isCurrentPoolShutDown = true;
                if (rs != null) {
                    try {
                        rs.close();
                    } catch (SQLException sqlex) {
                        logging.error(logPreString
                                + "Error closing statement: "
                                + sqlex.getMessage());
                    }
                }

                if (stmt != null) {
                    try {
                        stmt.close();
                    } catch (SQLException sqlex) {
                        logging.error(logPreString
                                + "Failed to close statement: "
                                + sqlex.getMessage());
                    }
                }

                if (conn != null) {
                    try {
                        conn.close();
                    } catch (SQLException sqle) {
                        logging.error(logPreString
                                + "Failed to close connection: "
                                + sqle.getMessage());
                    }
                }
            }
        }
    }

    /**
     * Creates a simple Runnable that holds a Job object thats worked on by the
     * child threads.
     *
     * @param outMessageID
     * @param message
     * @param source
     * @param destination
     * @param numofSends
     * @param nextSend
     * @param connectorRule
     *
     * @return a new StatusPusherJob task
     */
    private synchronized Runnable createTask(
            final String payerTransactionID,
            final int requestLogID,
            final int overallStatus, final int serviceID,
            final String receiptNumber, final String receiverNarration,
            final String clientCode,
            final String nextSend,
            final String firstSend, final String lastSend,
            final String url, final String protocol,
            final int sslEnabled, final String sslCertificatePath,
            final String username, final String password,
            final String method) {
        String logPreString = this.logPreString + "createTask() | -1 | ";
        logging.info(logPreString
                + "Creating a task for message with RequestLogID: "
                + requestLogID);
        return new StatusPusherJob(logging, props, mysql, payerTransactionID,
                requestLogID, overallStatus, serviceID, receiptNumber,
                receiverNarration, clientCode, nextSend, firstSend, lastSend,
                url, protocol, sslEnabled, sslCertificatePath, username,
                password, method);
    }

    /**
     * Process Records.
     */
    public void runDaemon() {
        String logPreString = this.logPreString + "runDaemon() | -1 | ";
        int pingState = PhillipsConstants.PING_FAILED;
        try {
            pingState = pingDatabaseServer(mysql.getConnection());
        } catch (SQLException ex) {
            logging.info(logPreString
                    + "Exception was caught when trying to ping DB. Error"
                    + ex.getMessage());
        }
        if (pingState == StatusPusherConstants.PING_SUCCESS) {
            // The database is available, allocate, fetch and reset the bucket
            if (daemonState == StatusPusherConstants.DAEMON_RUNNING) {
                doWork();
            } else if (daemonState == StatusPusherConstants.DAEMON_RESUMING) {

                doWait(props.getSleepTime());

                logging.info(logPreString + "Resuming daemon service...");
                daemonState = StatusPusherConstants.DAEMON_RUNNING;
                logging.info(logPreString
                        + "Daemon resumed successfully, working...");
            }
        } else {
            logging.error(logPreString + "The database server: "
                    + props.getDbHost() + " servicing on port: "
                    + props.getDbPort() + " appears to be down. Reason: "
                    + "internal function for pingDatabaseServer() returned a "
                    + "PING_FAILED status.");
            daemonState = StatusPusherConstants.DAEMON_INTERRUPTED;

            logging.info(logPreString + "Connection to the database was "
                    + "interrupted, suspending from service...");
            logging.info(logPreString + "Cleaning up service...");

            // Enter a Suspended state
            while (true) {
                if (daemonState == StatusPusherConstants.DAEMON_INTERRUPTED) {
                    int istate = StatusPusherConstants.PING_FAILED;
                    try {
                        istate = pingDatabaseServer(mysql.getConnection());
                    } catch (SQLException ex) {
                        logging.info(logPreString
                                + "Exception was caught when trying to ping DB. Error"
                                + ex.getMessage());
                    }
                    if (istate == StatusPusherConstants.PING_SUCCESS) {
                        daemonState = StatusPusherConstants.DAEMON_RESUMING;
                        break;
                    }
                }

                doWait(props.getSleepTime());
            }
        }
    }

    /**
     * A better functional logic that ensures secure execution of fetch bucket
     * as well as detailed management of interrupted queries. This will work
     * only when we have a db connection.
     */
    private synchronized void doWork() {
        rollbackSystem();
        executeTasks();
    }

    /**
     * Update successful transactions that were not updated.
     */
    private void rollbackSystem() {
        String logPreString = this.logPreString + "rollbackSystem() | -1 | ";
        List<String> failedQueries = checkForFailedQueries(
                StatusPusherConstants.FAILED_QUERIES_FILE);
        int failures = failedQueries.size();
        int recon = 0;

        if (failures > 0) {
            logging.info(logPreString + "I found " + failures
                    + " failed update queries in file: "
                    + StatusPusherConstants.FAILED_QUERIES_FILE
                    + ", rolling back transactions...");

            do {
                String recon_query = failedQueries.get(recon);
                doRecon(recon_query, StatusPusherConstants.RETRY_COUNT);
                //doWait(props.getSleepTime());
                recon++;
            } while (recon != failures);

            logging.info(logPreString
                    + "I have finished performing rollback...");
        }
    }

    /**
     * Loads a file with selected queries and re-runs them internally.
     *
     * @param file the file to check for failed queries
     */
    @SuppressWarnings("NestedAssignment")
    private List<String> checkForFailedQueries(final String file) {
        String logPreString = this.logPreString + "checkForFailedQueries() | -1 | ";
        List<String> queries = new ArrayList<String>(0);

        try {
            /*
             * If we fail to open the file, then the file has not been created
             * yet. This is good because it means that there is no error.
             */
            if (new File(file).exists()) {
                fin = new FileInputStream(file);
                in = new DataInputStream(fin);
                br = new BufferedReader(new InputStreamReader(in));

                String data;
                while ((data = br.readLine()) != null) {
                    if (!queries.contains(data)) {
                        queries.add(data);
                    }
                }
                br.close();
                fin.close();
                in.close();

            }
        } catch (Exception e) {
            logging.error(logPreString
                    + " Error reading from FAILED_QUERIES.TXT: " + e);
        }

        return queries;
    }

    /**
     * This function determines how the queries will be re-executed i.e. whether
     * SELECT or UPDATE.
     *
     * @param query the query to re-execute
     * @param tries the number of times to retry
     */
    private void doRecon(final String query, final int tries) {
        String logPreString = this.logPreString + "doRecon() | -1 | ";
        int maxRetry = props.getMaxFailedQueryRetries();
        if (query.toLowerCase().startsWith(StatusPusherConstants.UPDATE_ID)) {
            int qstate = runUpdateRecon(query);
            if (qstate == StatusPusherConstants.UPDATE_RECON_SUCCESS) {
                logging.info(logPreString + "Re-executed this query: "
                        + query + " successfully, deleting it from file...");
                deleteQuery(StatusPusherConstants.FAILED_QUERIES_FILE, query);
            } else if (qstate == StatusPusherConstants.UPDATE_RECON_FAILED) {
                logging.info(logPreString
                        + "Failed to re-execute failed query: " + query
                        + "[Try " + tries + " out of  " + maxRetry + "]");
                int curr_try = tries + 1;
                if (tries >= maxRetry) {
                    logging.info(logPreString
                            + "Tried to re-execute failed query "
                            + props.getMaxFailedQueryRetries()
                            + " times but still failed, exiting...");
                    System.exit(1);
                } else {
                    logging.info(logPreString + "Retrying in "
                            + (props.getSleepTime() / 1000) + " sec(s) ");
                    doWait(props.getSleepTime());
                    doRecon(query, curr_try);
                }
            }
        }
    }

    /**
     * Re-executes the specified query.
     *
     * @param query the query to run
     */
    private int runUpdateRecon(final String query) {
        String logPreString = this.logPreString + "runUpdateRecon() | -1 | ";
        int result = 0;
        Statement stmt = null;
        Connection conn = null;

        try {
            conn = mysql.getConnection();
            stmt = conn.createStatement();
            stmt.executeUpdate(query);
            logging.info(logPreString + "I have just successfully "
                    + "re-executed this failed query: " + query);
            result = StatusPusherConstants.UPDATE_RECON_SUCCESS;

            stmt.close();
            stmt = null;
            conn.close();
            conn = null;
        } catch (SQLException e) {
            logging.error(logPreString + "SQLException: " + e.getMessage());
            result = StatusPusherConstants.UPDATE_RECON_FAILED;
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException sqlex) {
                    logging.error(logPreString + "runUpdateRecon --- "
                            + "Failed to close Statement object: "
                            + sqlex.getMessage());
                }
            }

            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException sqle) {
                    logging.error(logPreString + "runUpdateRecon --- "
                            + "Failed to close connection object: "
                            + sqle.getMessage());
                }
            }
        }

        return result;
    }


    /*--Delete a query from the failed_queries file after a successfull recon--*/
    public void deleteQuery(String queryfile, String query) {
        String logPreString = this.logPreString + "deleteQuery() | -1 | ";
        ArrayList<String> queries = new ArrayList<String>();
        try {
            fin = new FileInputStream(queryfile);
            in = new DataInputStream(fin);
            br = new BufferedReader(new InputStreamReader(in));

            String data = null;
            while ((data = br.readLine()) != null) {
                queries.add(data);
            }
            br.close();
            fin.close();
            in.close();
            /*--Find a match to the query--*/
            logging.info(logPreString + "About to remove this query: " + query
                    + " from file: " + queryfile);
            if (queries.contains(query)) {
                queries.remove(query);
                logging.info(logPreString + "I have removed this query: " + query
                        + " from file: " + queryfile);
            }
            /*--Now save the file--*/
            pout = new PrintWriter(new FileOutputStream(queryfile, false));
            for (String new_queries : queries) {
                pout.println(new_queries);
            }
            pout.close();
        } catch (Exception e) {
            /**
             * If we fail to open it then, the file has not been created yet'
             * This is good because it means that no error(s) have been
             * experienced yet
             */
            logging.error(logPreString
                    + "Error while deleting query from file. "
                    + "Exception message: " + e.getMessage());
        }
    }

    /* --Sleep-Time -- */
    public void doWait(long t) {
        String logPreString = this.logPreString + "doWait() | -1 | ";
        try {
            Thread.sleep(t);
        } catch (InterruptedException ex) {
            logging.error(logPreString
                    + "Thread could not sleep fpr the specified time");

            /* --DO NOTHING--*/ }
    }

    /**
     * Checks if the database server is up.
     *
     * @return state of database connection
     */
    private int pingDatabaseServer(Connection conn) {
        String logPreString = this.logPreString + "pingDatabaseServer() | -1 | ";

        int state = 0;
        String query = "SELECT 1 AS TEST FROM dual;";
        PreparedStatement preparedStatement = null;
        ResultSet rs = null;

        try {
            preparedStatement = conn.prepareStatement(query);
            rs = preparedStatement.executeQuery();
            rs.next();
            int status = rs.getInt("TEST");
            if (status == 1) {
                logging.info(logPreString + "Ping Successful proceeding...");
                state = StatusPusherConstants.PING_SUCCESS;
            } else {
                logging.info(logPreString + "Pinging Failed, will retry...");
                state = StatusPusherConstants.PING_FAILED;
            }
            rs.close();
            rs = null;
            preparedStatement.close();
            preparedStatement = null;
            conn.close();
            conn = null;
        } catch (SQLException ex) {
            logging.fatal(logPreString + "SQLException Thrown => "
                    + ex.getMessage());
            state = StatusPusherConstants.PING_FAILED;
        } catch (Exception ex) {
            logging.fatal(logPreString + "Exception Thrown => "
                    + ex.getMessage());
            state = StatusPusherConstants.PING_FAILED;
        } finally {
            try {

                if (rs != null) {
                    rs.close();
                }

                if (preparedStatement != null) {
                    preparedStatement.close();
                }

                if (conn != null) {
                    conn.close();
                }

            } catch (Exception ex) {
                logging.fatal(logPreString + "closeConnection() ---"
                        + "Exception thrown while trying to free resources."
                        + ex.getMessage());
            }

        }

        return state;
    }

    /**
     * The following method shuts down an ExecutorService in two phases, first
     * by calling shutdown to reject incoming tasks, and then calling
     * shutdownNow, if necessary, to cancel any lingering tasks (after 6
     * minutes).
     *
     * @param pool the executor service pool
     */
    private void shutdownAndAwaitTermination(final ExecutorService pool) {
        String logPreString = this.logPreString + "shutdownAndAwaitTermination() | -1 | ";
        logging.info(logPreString
                + "Executor pool waiting for tasks to complete");
        pool.shutdown(); // Disable new tasks from being submitted

        try {
            // Wait a while for existing tasks to terminate
            if (!pool.awaitTermination(6, TimeUnit.MINUTES)) {
                logging.error(logPreString
                        + "Executor pool  terminated with tasks "
                        + "unfinished. Unfinished tasks will be retried.");
                pool.shutdownNow(); // Cancel currently executing tasks

                // Wait a while for tasks to respond to being cancelled
                if (!pool.awaitTermination(6, TimeUnit.MINUTES)) {
                    logging.error(logPreString
                            + "Executor pool terminated with tasks "
                            + "unfinished. Unfinished tasks will be retried.");
                }
            } else {
                logging.info(logPreString + "Executor pool completed all tasks and has shut "
                        + "down normally");
            }
        } catch (InterruptedException ie) {
            logging.error(logPreString
                    + "Executor pool shutdown error: " + ie.getMessage());
            // (Re-)Cancel if current thread also interrupted
            pool.shutdownNow();

            // Preserve interrupt status
            Thread.currentThread().interrupt();
        }

        isCurrentPoolShutDown = true;
    }

    /**
     * Set the new push information
     *
     * @param pushInfo
     */
    public void setPushInfo(PushProps pushInfo) {
        this.pushInfo = pushInfo;
    }

    /**
     * Gets whether the current pool has been shut down.
     *
     * @return whether the current pool has been shut down
     */
    public boolean getIsCurrentPoolShutDown() {
        return isCurrentPoolShutDown;
    }
}