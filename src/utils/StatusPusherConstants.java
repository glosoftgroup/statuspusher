package com.cellulant.statusPusher.utils;

/**
 * Constants used in the daemon.
 *
 * @author <a href="brian.ngure@cellulant.com">Brian Ngure</a>
 */
@SuppressWarnings({ "ClassMayBeInterface", "FinalClass" })
public final class StatusPusherConstants {
    /**
     * Private constructor.
     */
    private StatusPusherConstants() {
    }

    /**
     * Maximum size of a log file.
     */
    public static final String MAX_LOG_FILE_SIZE = "500GB";
    /**
     * Maximum number of log files.
     */
    public static final int MAX_NUM_LOGFILES = 4;
    // Constants
    public static final int PING_FAILED = 101;
    public static final int PING_SUCCESS = 100;
    public static final String FAILED_QUERIES_FILE = "FAILED_QUERIES.TXT";
    public static final int UPDATE_RECON_SUCCESS = 102;
    public static final int UPDATE_RECON_FAILED = 103;
    public static final int DAEMON_RUNNING = 1005;
    public static final int DAEMON_INTERRUPTED = 1006;
    public static final int DAEMON_RESUMING = 1007;
    public static final int SINGLE_INSTANCE = 1009;
    public static final int MULTI_INSTANCE = 1010;
    public static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
    public static final String TIME_FORMAT = "HH:mm:ss";
    public static final String UPDATE_ID = "update";

    public static final int RETRY_COUNT = 1;
}