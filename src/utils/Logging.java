 package com.cellulant.statusPusher.utils;

import java.io.IOException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;

/**
 * Initializes the log files.
 *
 * @author <a href="brian.ngure@cellulant.com">Brian Ngure</a>
 */
@SuppressWarnings("FinalClass")
public class Logging {

    /**
     * Info log.
     */
    private static Logger infoLog;
    /**
     * Error log.
     */
    private static Logger errorLog;
    /**
     * Loaded system properties.
     */
    private transient Props props;

    /**
     * Constructor.
     *
     * @param properties passed in loaded system properties
     */
    public Logging(final Props properties) {
        this.props = properties;
        initializeLoggers();
    }

    /**
     * Initialize the log managers.
     */
    @SuppressWarnings({"CallToThreadDumpStack", "UseOfSystemOutOrSystemErr"})
    private void initializeLoggers() {
        infoLog = Logger.getLogger("infoLog");
        errorLog = Logger.getLogger("errorLog");

        PatternLayout layout = new PatternLayout();
        layout.setConversionPattern(
            "%d{ISO8601} | %p | %m%n");

        try {
            RollingFileAppender rfaInfoLog = new RollingFileAppender(layout,
                props.getInfoLogFile(), true);

            rfaInfoLog.setMaxFileSize(StatusPusherConstants.MAX_LOG_FILE_SIZE);
            rfaInfoLog.setMaxBackupIndex(
                StatusPusherConstants.MAX_NUM_LOGFILES);

            RollingFileAppender rfaErrorLog = new RollingFileAppender(layout,
                props.getErrorLogFile(), true);
            rfaErrorLog.setMaxFileSize(StatusPusherConstants.MAX_LOG_FILE_SIZE);
            rfaErrorLog
                .setMaxBackupIndex(StatusPusherConstants.MAX_NUM_LOGFILES);

            infoLog.addAppender(rfaInfoLog);
            errorLog.addAppender(rfaErrorLog);
        } catch (IOException ex) {
            System.err.println("Failed to initialize loggers... EXITING: "
                + ex.getMessage());
            ex.printStackTrace();
            System.exit(1);
        }

        infoLog.setLevel(Level.toLevel(props.getInfoLogLevel()));
        errorLog.setLevel(Level.toLevel(props.getErrorLogLevel()));

        info("Just finished initializing Loggers...");
    }

    /**
     * Log info messages.
     *
     * @param message the message content
     */
    public void info(final String message) {

        infoLog.info(message);
    }

    /**
     * Log debug messages.
     *
     * @param message the message content
     */
    public void debug(final String message) {

        infoLog.debug(message);
    }

    /**
     * Log error messages.
     *
     * @param message the message content
     */
    public void error(final String message) {

        errorLog.error(message);
    }

    /**
     * Log fatal error messages.
     *
     * @param message the message content
     */
    public void fatal(final String message) {

        errorLog.fatal(message);
    }
} 