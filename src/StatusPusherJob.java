package com.cellulant.statusPusher;

import com.cellulant.statusPusher.db.MySQL;
import com.cellulant.statusPusher.utils.StatusPusherConstants;
import com.cellulant.statusPusher.utils.Logging;
import com.cellulant.statusPusher.utils.Props;
import com.cellulant.statusPusher.utils.Utilities;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * The actual object whose functions are to be executed by the child thread.
 *
 * @author Daniel Mbugua
 */
@SuppressWarnings("FinalClass")
public final class StatusPusherJob implements Runnable {

    /**
     * The MySQL connection object.
     */
    private transient MySQL mysql;
    /**
     * Logger for this class.
     */
    private transient Logging logging;
    /**
     * The properties instance.
     */
    private transient Props props;
    /**
     * The client code as stored in the DB.
     */
    private transient String clientCode;
    /**
     * The the reuestLogID in the request Log table.
     */
    private transient int requestLogID;
    /**
     * The payerTransactionID of the as stored in payments table.
     */
    private transient String payerTransactionID;
    /**
     * The overall status of the request.
     */
    private transient int overallStatus;
    /**
     * The string to append before the string being logged.
     */
    private String logPreString;
    /**
     * Time when the status should be next sent.
     */
    private transient String nextSend;
    /**
     * Time when the status was last sent.
     */
    private transient String firstSend;
    /**
     * Time when the status was last sent.
     */
    private transient String lastSend;
    /**
     * Flag to show if the API is SSL Enabled.
     */
    private transient int sslEnabled;
    /**
     * Protocol used to access API.
     */
    private transient String protocol;
    /**
     * The URL to the API.
     */
    private transient String url;
    /**
     * Path to the SSL Certificate.
     */
    private transient String sslCertificatePath;
    /**
     * Username to access the API.
     */
    private transient String username;
    /**
     * method to push to.
     */
    private transient String method;
    /**
     * Password to access the API.
     */
    private transient String password;
    /**
     * serviceID of the transaction.
     */
    private transient int serviceID;
    /**
     * Receipt from merchant after acknowledgment.
     */
    private transient String receiptNumber;
    /**
     * Narration from merchant after acknowledgment.
     */
    private transient String receiverNarration;

    /**
     * Constructor.
     *
     * @param logging
     * @param props
     * @param mySQL
     * @param payerTransactionID the payment ID
     * @param requestLogID
     * @param overallStatus
     * @param clientCode
     */
    public StatusPusherJob(
            final Logging logging, final Props props,
            final MySQL mySQL, final String payerTransactionID,
            final int requestLogID, final int overallStatus,
            final int serviceID, final String receiptNumber,
            final String receiverNarration,
            final String clientCode, final String nextSend,
            final String firstSend, final String lastSend,
            final String url, final String protocol,
            final int sslEnabled, final String sslCertificatePath,
            final String username, final String password,
            final String method) {

        this.logging = logging;
        this.props = props;
        this.mysql = mySQL;
        this.clientCode = clientCode;
        this.requestLogID = requestLogID;
        this.overallStatus = overallStatus;
        this.payerTransactionID = payerTransactionID;
        this.nextSend = nextSend;
        this.firstSend = firstSend;
        this.lastSend = lastSend;
        this.url = url;
        this.protocol = protocol;
        this.sslEnabled = sslEnabled;
        this.sslCertificatePath = sslCertificatePath;
        this.username = username;
        this.password = password;
        this.method = method;
        this.serviceID = serviceID;
        this.receiverNarration = receiverNarration;
        this.receiptNumber = receiptNumber;
        this.logPreString = "PushPaymentStatusJob | ";
    }

    /**
     * This is the method called when this task is run. It creates a client
     * request to the server, gets and processes the response.
     */
    @SuppressWarnings({"SleepWhileInLoop", "SleepWhileHoldingLock"})
    public void processRequest() {
        String logPreString = this.logPreString + "processRequest() | "
                + requestLogID + " | ";

        /**
         * The JSON Reply from Hub.
         */
        String jsonReply = "";
        /**
         * The status code to update the record.
         */
        int statusCode = 0;

        /**
         * The status code from Hub.
         */
        int retStatusCode = 0;
        /**
         * The returned requestLogID or client_sms_id.
         */
        int retRequestLogID = 0;
        /**
         * The returned payerTransactionID.
         */
        String retPayerTID = "";
        /**
         * The status description from Pusher Wrappers.
         */
        String retStatusDesc = "";
        /**
         * Status Description to send to client.
         */
        String sendStatusDescription = "";
        /**
         * Status Description to send to client.
         */
        int sendStatus = 0;

        HttpPost httppost = null;
        HttpParams httpParams = null;
        HttpClient httpclient = null;
        HttpResponse response = null;

        try {

            if (overallStatus == props.getPaymentAcceptedCode()) {
                sendStatus = props.getPaymentAcceptedPushCode();
                sendStatusDescription = "Payment has been accepted by merchant.";
            } else if (overallStatus == props.getPaymentRejectedCode()) {
                sendStatus = props.getPaymentRejectedPushCode();
                sendStatusDescription = "Payment has been rejected by merchant.";
            } else if (overallStatus == props.getPendingReversalCode()
                    || overallStatus == props.getPaymentEscalatedCode()
                    || overallStatus == props.getUnknownStatusCode()) {
                sendStatus = props.getPaymentEscalatedCode();
                sendStatusDescription = "Payment has been escalated for manual "
                        + "reconciliation.";
            } else if (overallStatus == props.getTransactionManuallySuccessfulCode()) {
                sendStatus = props.getTransactionManuallySuccessfulCode();
                sendStatusDescription = "Payment has been manually reconciled "
                        + "and is accepted";
            } else if (overallStatus == props.getTransactionManuallyFailedCode()) {
                sendStatus = props.getTransactionManuallyFailedCode();
                sendStatusDescription = "Payment has been manually reconciled "
                        + "and is rejected";
            } else {
                this.updateTransaction(requestLogID,
                        props.getUnprocessedStatus(), "The overall status provided "
                        + "cannot be processed in this situation");
                return;
            }

            if (!username.isEmpty() || !password.isEmpty()) {

                httppost = new HttpPost(props.getWrapperScript());
                httpParams = new BasicHttpParams();
                HttpConnectionParams.setConnectionTimeout(httpParams,
                        props.getConnectionTimeout());
                HttpConnectionParams.setSoTimeout(httpParams,
                        props.getReplyTimeout());
                httpclient = new DefaultHttpClient(httpParams);

                logging.info(logPreString
                        + "Formulating post Parameters - Client URL: " + url
                        + ", Beep Transaction ID: " + requestLogID
                        + ", Payer Transaction ID: " + payerTransactionID
                        + ", Client Protocol: " + protocol + ", Push Username: "
                        + username + ", Push Password: ************, "
                        + "Receiver narration: " + receiverNarration
                        + ", Receipt Number: " + receiptNumber);

// Request parameters and other properties.
                List<NameValuePair> params = new ArrayList<>(2);
                params.add(new BasicNameValuePair("url",
                        String.valueOf(url)));
                params.add(new BasicNameValuePair("clientCode",
                        String.valueOf(clientCode)));
                params.add(new BasicNameValuePair("method",
                        String.valueOf(method)));
                params.add(new BasicNameValuePair("protocol",
                        String.valueOf(protocol)));
                params.add(new BasicNameValuePair("externalUsername",
                        String.valueOf(username)));
                params.add(new BasicNameValuePair("externalPassword",
                        String.valueOf(password)));
                params.add(new BasicNameValuePair("serviceID",
                        String.valueOf(serviceID)));
                params.add(new BasicNameValuePair("sslEnabled",
                        String.valueOf(sslEnabled)));
                params.add(new BasicNameValuePair("sslCertificatePath",
                        String.valueOf(sslCertificatePath)));
                params.add(new BasicNameValuePair("beepTransactionID",
                        String.valueOf(requestLogID)));
                params.add(new BasicNameValuePair("payerTransactionID",
                        String.valueOf(payerTransactionID)));
                params.add(new BasicNameValuePair("receiverNarration",
                        String.valueOf(receiverNarration)));
                params.add(new BasicNameValuePair("receiptNumber",
                        String.valueOf(receiptNumber)));
                params.add(new BasicNameValuePair("statusCode",
                        String.valueOf(sendStatus)));
                params.add(new BasicNameValuePair("statusDescription",
                        sendStatusDescription));

                httppost.setEntity(new UrlEncodedFormEntity(params, "UTF-8"));

//Execute and get the response.
                response = httpclient.execute(httppost);

                logging.info(logPreString
                        + "Response from the Wrappers: "
                        + response);
                if (response != null) {
                    InputStreamReader in = new InputStreamReader(
                            response.getEntity().getContent());
                    BufferedReader br = new BufferedReader(in);

                    jsonReply = br.readLine();
                    jsonReply = jsonReply.trim();

                    br.close();
                    in.close();
                    logging.info(logPreString
                            + "JSON Response from the the Wrappers: "
                            + jsonReply);

                    if (!jsonReply.isEmpty()) {
                        JSONObject jsonResp = new JSONObject(jsonReply);

                        retStatusCode = jsonResp.getInt("statusCode");

                        retStatusDesc = jsonResp.optString(
                                "statusDescription",
                                "No status description from wrapper");

                        retRequestLogID = jsonResp.optInt("beepTransactionID",
                                0);
                        retPayerTID = jsonResp.optString(
                                "payerTransactionID");

                        logging.info(logPreString
                                + "The status code returned for record with "
                                + "BeepTransactionID " + retRequestLogID
                                + " and Payer Transaction ID "
                                + retPayerTID + " is '"
                                + retStatusCode + "' and the status "
                                + "description is '" + retStatusDesc + "'");

                        //check if the message was processed successfully and
                        //it is the message thatwas sent
                        if (retRequestLogID == requestLogID) {
                            if (retStatusCode
                                    == props.getSuccessfullyDelivered()) {
                                statusCode = props.getProcessedStatus();
                            } else if (retStatusCode
                                    == props.getFailedToDeliver()) {
                                statusCode = props.getUnprocessedStatus();
                            }
                        } else if (retRequestLogID != requestLogID) {
                            statusCode = props.getEscalatedStatus();
                        } else {
                            statusCode = props.getUnprocessedStatus();
                        }

                        retRequestLogID = this.requestLogID;
                    } else {
                        logging.error(logPreString
                                + "The wrapper invocation returned a "
                                + "response but was empty.");
                        statusCode = props.getUnprocessedStatus();
                    }

                    response = null;
                    httppost = null;
                    httpclient = null;
                } else {
                    statusCode = props.getUnprocessedStatus();
                    retRequestLogID = this.requestLogID;

                    logging.error(logPreString
                            + "The Wrapper invocation failed to return a "
                            + "response.");
                }
            } else {
                logging.error(logPreString
                        + "Either the username or password is empty Please ensure "
                        + "the username and password to invoke the beep API are "
                        + "set and the password can be decrypted. Password "
                        + password);
            }

        } catch (ClientProtocolException ex) {
            logging.error(logPreString
                    + "An ClientProtocolException has been caught while "
                    + "invoking the Pusher Wrappers. Error Message: "
                    + ex.getMessage());
            statusCode = props.getUnprocessedStatus();
            retRequestLogID = this.requestLogID;
        } catch (UnsupportedEncodingException ex) {
            logging.error(logPreString
                    + "An UnsupportedEncodingException has been caught "
                    + "while invoking the Pusher Wrappers. Error Message: "
                    + ex.getMessage());
            statusCode = props.getUnprocessedStatus();
            retRequestLogID = this.requestLogID;
        } catch (IOException ex) {
            logging.error(logPreString
                    + "An IOException has been caught while invoking the "
                    + "Pusher Wrappers. Error Message: " + ex.getMessage());
            statusCode = props.getUnprocessedStatus();
            retRequestLogID = this.requestLogID;
        } catch (JSONException ex) {
            logging.error(logPreString
                    + "A JSONException has been caught while decoding the "
                    + "reply." + jsonReply + " Error Message: "
                    + ex.getMessage());
            statusCode = props.getUnprocessedStatus();
            retRequestLogID = this.requestLogID;
        } catch (Exception ex) {
            logging.error(logPreString
                    + "A " + ex.getClass() + " has been caught while "
                    + "processing status.  Error Message: " + ex.getMessage());
            statusCode = props.getUnprocessedStatus();
            retRequestLogID = this.requestLogID;
        } finally {
            response = null;
            httppost = null;
            httpclient = null;
            httpParams = null;
        }

        this.updateTransaction(retRequestLogID,
                statusCode, retStatusDesc);
    }

    /**
     * Acknowledges the transaction.
     *
     * @param returnedRequestLogID The out message ID returned as client sms ID
     * @param statusCode status code to update to
     * @param statusDescription status description
     * @param hubRefID the hub reference ID
     *
     * @return true if successful, false otherwise
     */
    private void updateTransaction(final int returnedRequestLogID,
            int statusCode, String statusDesc) {

        String logPreString = this.logPreString + "updateTransaction() | "
                + requestLogID + " | ";
        String query = "";

        if (statusDesc.length() > 99) {
            statusDesc = statusDesc.substring(0, 95) + "...";
        }
        PreparedStatement stmt = null;
        Connection conn = null;
        if (nextSend != null) {
            Calendar ackTimeout = Calendar.getInstance();
            Calendar nextSendDate = Calendar.getInstance();

            SimpleDateFormat sdf = new SimpleDateFormat(StatusPusherConstants.DATE_FORMAT);
            try {
                ackTimeout.setTime(sdf.parse(firstSend));
                ackTimeout.add(Calendar.SECOND, props.getPushAckTimeoutPeriod());

                nextSendDate.setTime(sdf.parse(nextSend));
                nextSendDate.add(Calendar.SECOND, props.getNextSendInterval());
            } catch (ParseException ex) {
                logging.error(logPreString
                        + "A parse exception occured while trying to compare "
                        + "dates. Error:  " + ex.getMessage());
            }

            if (nextSendDate.compareTo(ackTimeout) > 0
                    && statusCode == props.getUnprocessedStatus()) {
                statusCode = props.getFailedStatus();
            }
        }

        statusDesc = statusDesc.replace('\"', '\'');
        String addQuery = "";
        String trueQuery = "";

        if (firstSend == null) {
            addQuery = ", statusFirstSend = NOW() ";

        }

        String[] params = {
            String.valueOf(statusCode),
            String.valueOf(statusDesc),
            String.valueOf(props.getNextSendInterval()),
            String.valueOf(returnedRequestLogID)
        };

        query = "UPDATE s_payments SET statusPushed = ?, statusPushedDesc = ?, "
                + "statusNextSend = DATE_ADD( NOW(), INTERVAL ? SECOND), "
                + "statusLastSend = NOW() " + addQuery + " WHERE requestLogID = ?";

        trueQuery = Utilities.prepareSqlString(query, params, 0);

        try {

            conn = mysql.getConnection();

            stmt = conn.prepareStatement(query);
            stmt.setInt(1, statusCode);
            stmt.setString(2, statusDesc);
            stmt.setInt(3, props.getNextSendInterval());
            stmt.setInt(4, returnedRequestLogID);

            logging.info(logPreString
                    + "Updating Record with requestLogID: "
                    + returnedRequestLogID + " to status " + statusCode);

            logging.info(logPreString
                    + "Update query: " + trueQuery);

            stmt.executeUpdate();

            logging.info(logPreString
                    + "Record processed with requestLogID: "
                    + returnedRequestLogID);

            stmt.close();
            stmt = null;
            conn.close();
            conn = null;
        } catch (Exception ex) {

            logging.error(logPreString
                    + "An error occured while updating the Record with "
                    + "RequestLog ID: " + returnedRequestLogID + ". Error: "
                    + ex.getMessage());
            query = "UPDATE s_payments SET statusPushed = ?, statusPushedDesc "
                    + "= \"?\", statusNextSend = DATE_ADD( NOW(), INTERVAL ? SECOND), "
                    + "statusLastSend = NOW() " + addQuery + " WHERE "
                    + "requestLogID = ?";

            String trueStoreQuery = Utilities.prepareSqlString(query, params, 0);
            Utilities.updateFile(StatusPusherConstants.FAILED_QUERIES_FILE,
                    trueStoreQuery);
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                    stmt = null;
                } catch (Exception ex) {
                    logging.error(logPreString
                            + "Failed to close Statement object: "
                            + ex.getMessage());
                }
            }

            if (conn != null) {
                try {
                    conn.close();
                    conn = null;
                } catch (Exception ex) {
                    logging.error(logPreString
                            + "Failed to close connection object: "
                            + ex.getMessage());
                }
            }
        }
    }

    /**
     * Runs the task.
     */
    @Override
    public void run() {

        this.processRequest();

    }
}