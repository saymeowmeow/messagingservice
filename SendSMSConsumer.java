import com.rabbitmq.client.*;
import com.rabbitmq.client.ConnectionFactory;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.json.JSONException;
import org.json.JSONObject;

import com.google.gson.Gson;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.rabbitmq.client.Channel;
import okhttp3.*;
import okhttp3.FormBody.Builder;
import okhttp3.MediaType;
import okhttp3.MultipartBody;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;


public class SendSMSConsumer {
    private static final String QUEUE_NAME = "sms_queue";
    private static final String DB_URL = "jdbc:edb://10.10.10.54:5447/odisha_uat";
    private static final String DB_USER = "odisha_uat";
    private static final String DB_PASSWORD = "Od15hauat";
    
 // Add your FCM server key here
    private static final String FCM_API_URL = "https://fcm.googleapis.com/fcm/send";
    private static final String FCM_AUTH_KEY = "AAAAu3aY5sk:APA91bHoiUlAeKDqc4jN5QlWP-mlFjYKLpVbk4bvKvp20NbxWimquU2AmydAbu4o8H1HiV5_TQXQuSq6Vta_De9WRXh3QlzhflT_8Llyr07eh2M_DoEJ0G09gAjCIbSWhy_BsXpUZo5P";
    
    private static final int MAX_RETRIES = 3;
    private static final long RETRY_INTERVAL_MS = 6000; // 6 second (adjust as needed)

    
    private static class ApiResponse {
        private int status;

        public int getStatus() {
            return status;
        }
    }
    // Define classes for FCM notification payload
    private static class FcmNotificationPayload {
        private String to;
        private FcmNotificationData data;
        private FcmNotification notification;

        // Getters and setters
        public String getTo() {
            return to;
        }

        public void setTo(String to) {
            this.to = to;
        }

        public FcmNotificationData getData() {
            return data;
        }

        public void setData(FcmNotificationData data) {
            this.data = data;
        }

        public FcmNotification getNotification() {
            return notification;
        }

        public void setNotification(FcmNotification notification) {
            this.notification = notification;
        }
    }

    private static class FcmNotificationData {
        private String url;
        private String val;

        // Constructor, getters and setters
        public FcmNotificationData(String url, String val) {
            this.url = url;
            this.val = val;
        }

        public String getUrl() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }

        public String getVal() {
            return val;
        }

        public void setVal(String val) {
            this.val = val;
        }
    }

    private static class FcmNotification {
        private String body;
        private String title;

        // Constructor, getters and setters
        public FcmNotification(String body, String title) {
            this.body = body;
            this.title = title;
        }

        public String getBody() {
            return body;
        }

        public void setBody(String body) {
            this.body = body;
        }

        public String getTitle() {
            return title;
        }

        public void setTitle(String title) {
            this.title = title;
        }
    }


    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("10.226.30.117");
        factory.setPort(5672);
        factory.setUsername("newadmin");
        factory.setPassword("Cdac@123");

        com.rabbitmq.client.Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);

        System.out.println("Waiting for messages... \n");

        channel.basicConsume(QUEUE_NAME, false, new DefaultConsumer(channel) {
        	
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, StandardCharsets.UTF_8);
//                remove comment to print messages received by queue
//                System.out.println("Received message: " + message);

                // Print the properties object and its headers
//                remove comment to print properties
//                System.out.println("Properties: " + properties);
//                System.out.println("Headers: " + properties.getHeaders());

                // Create the HTTP request and send it to the API
                OkHttpClient client = new OkHttpClient().newBuilder().build();

                // Construct the form data
                String templateId = getStringHeaderValue(properties, "template_id");
                String smsContent = getStringHeaderValue(properties, "sms_content");
//                String phoneNumber = getStringHeaderValue(properties, "phonenumber");
                String phoneNumberValue = getStringHeaderValue(properties, "phonenumber");
                String action = getStringHeaderValue(properties, "action");
                String departmentId = getStringHeaderValue(properties, "department_id");
                
//                remove comment to print properties
//                System.out.println("templateId: " + templateId);
//                System.out.println("smsContent: " + smsContent);
//                System.out.println("phoneNumber: " + phoneNumber);
//                System.out.println("action: " + action);
//                System.out.println("departmentId: " + departmentId);

                FormBody.Builder requestBodyBuilder = new FormBody.Builder();

                if (templateId != null) {
                    requestBodyBuilder.add("template_id", templateId);
                }
                if (smsContent != null) {
                    requestBodyBuilder.add("sms_content", smsContent);
                }
                if (phoneNumberValue != null) {
                    requestBodyBuilder.add("phonenumber", phoneNumberValue);
                }
                if (action != null) {
                    requestBodyBuilder.add("action", action);
                }
                if (departmentId != null) {
                    requestBodyBuilder.add("department_id", departmentId);
                }

                RequestBody requestBody = requestBodyBuilder.build();
                StringBuilder payloadBuilder = new StringBuilder();
                String payload = "";  // Declaration and initialization of the payload variable

                // Print the key-value pairs of the FormBody object
                if (requestBody instanceof FormBody) {
                    FormBody formBody = (FormBody) requestBody;

                    // Build the payload string with key-value pairs separated by "&"
                    for (int i = 0; i < formBody.size(); i++) {
                        payloadBuilder.append(formBody.name(i))
                                .append("=")
                                .append(formBody.value(i));
                        if (i < formBody.size() - 1) {
                            payloadBuilder.append("&");
                        }
                    }
                    payload = payloadBuilder.toString();  // Assign the value to the payload variable

                    // Print the key-value pairs of the FormBody object
//                    for (int i = 0; i < formBody.size(); i++) {
//                        System.out.println("Parameter " + (i + 1) + ": " + formBody.name(i) + "=" + formBody.value(i));
//                    }
                }
                System.out.println("Request Payload:\n" + payload);
                Request request = new Request.Builder()
                        .url("https://govtsms.odisha.gov.in/api/api.php?")
                        .post(requestBody)
                        .build();

                // Set the request payload as the body of the HTTP request
                request = request.newBuilder()
                    .header("Content-Type", "application/x-www-form-urlencoded")
                    .build();
                
                // Send the HTTP request and process the response
                int retryCount = 0;
                while (retryCount < MAX_RETRIES) {
                try (Response response = client.newCall(request).execute()) {
                    if (response.isSuccessful()) {
                        // The API request was successful, acknowledge the message
                        channel.basicAck(envelope.getDeliveryTag(), false);
                        String responseBody = response.body().string();
                        System.out.println("API response: \n" + responseBody);
                        
                     // Extract the status from the API response
                        int status = extractStatusFromApiResponse(responseBody);

                     // Get the phone number from the properties
                        String phoneNumber = getStringHeaderValue(properties, "phonenumber");

                        // Update the database with the received status for the corresponding phone number
                        updateStatusInDatabase(phoneNumber, status);
                        break; // Break out of the retry loop if successful
                    } else {
                        // The API request failed, handle the error accordingly
                        System.out.println("API request failed: \n" + response.code() + " " + response.message());
                        // Optionally, you can choose to requeue the message for retry or handle the error in a different way
                        // channel.basicNack(envelope.getDeliveryTag(), false, true);
                    }
                } catch (IOException e) {
                	// Handle the exception and retry
                    retryCount++;
                    if (retryCount == MAX_RETRIES) {
                    	// Publish the message to the dead letter queue
                        channel.basicPublish("", "sms_queue_dlq", properties, body);
                        System.out.println("Max retries reached. Error sending API request. Message sent to dead letter queue");
                    } else {
                        System.out.println("Error sending API request. Retrying... (Attempt " + retryCount + ")");
                        try {
                            Thread.sleep(RETRY_INTERVAL_MS); // Wait before the next retry
                        } catch (InterruptedException ex) {
                            Thread.currentThread().interrupt();
                        }
                    }
                }
            }
             // Call the extractCRNoFromMessage method and pass the message
            String crNo = extractCRNoFromPayload(payload);
            if (crNo != null) {
                    // Process the CR No (e.g., filter data, get token ID, etc.)
            	System.out.println("Extracted CR No: " + crNo); 
            	String tokenID = getTokenIDFromDatabase(crNo);
                if (tokenID != null) {
                    System.out.println("Sending FCM notification to CR No: " + crNo + ", Token ID: " + tokenID);
                    // Send notifications via FCM using the retrieved token ID
                    sendFcmNotification(tokenID);
                } else {
                    System.out.println("Token ID not found for CR No: " + crNo);
                }
             } else {
                    System.out.println("CR No not found in message: " + message);
             }


            }
            private String extractCRNoFromPayload(String payload) {
                // Regular expression pattern to match the CR No format "CRNO: <CR_NUMBER>"
                // Change this pattern if your actual format is different
                String regexPattern = "CRNO:\\s*\\D*(\\d+)";
                Pattern pattern = Pattern.compile(regexPattern);
                Matcher matcher = pattern.matcher(payload);

                // Check if the pattern is found in the message content
                if (matcher.find()) {
                    // Extract and return the CR No from the matched group (group 1)
                    return matcher.group(1);
                } else {
                    // If the pattern is not found, return null or an appropriate value based on your use case
                    return null;
                }
            }
            
            private void processCRNo(String crNo) {
                // Assuming you have a method to retrieve the token ID from the 'mobile_patient_registration_dtl' table
                String tokenID = getTokenIDFromDatabase(crNo);

                if (tokenID != null) {
                	System.out.println("Sending FCM notification to CR No: " + crNo + ", Token ID: " + tokenID);
                    // Send notifications via FCM using the retrieved token ID
                    sendFcmNotification(tokenID);
                } else {
                    System.out.println("Token ID not found for CR No: " + crNo);
                }
            }
            private String getTokenIDFromDatabase(String crNo) {
                // Implement the database query to fetch the token ID based on the CR No
                // You need to connect to the database, execute the query, and return the token ID
                // For security reasons, I recommend using PreparedStatements with parameterized queries for database operations
                // The following is just a sample implementation; replace it with the actual database query logic
                String tokenID = null;
                Connection conn = null;
                PreparedStatement stmt = null;
                ResultSet rs = null;

                try {
                    conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
                 // Prepare the SQL query
                    String query = "SELECT hrgstr_token_id FROM mobile_patient_registration_dtl WHERE hrgnum_puk = ? ORDER BY gdt_entry_date DESC LIMIT 1";
                    stmt = conn.prepareStatement(query);
                    stmt.setString(1, crNo);
                 // Execute the query and get the result set
                    rs = stmt.executeQuery();
                    if (rs.next()) {
                        tokenID = rs.getString("hrgstr_token_id");
                    }
                    else {
                        System.out.println("Token ID not found for CR No: " + crNo);
                    }
                    rs.close();
                    stmt.close();
                    conn.close();
                } catch (SQLException e) {
                	 System.out.println("Error closing database resources: " + e.getMessage());
                }
                return tokenID;
            }
            private int extractStatusFromApiResponse(String responseBody) {
                // Parse the JSON response to extract the "status" field
                try {
                    JSONObject jsonObject = new JSONObject(responseBody);
                    return jsonObject.getInt("status");
                } catch (JSONException e) {
                    System.out.println("Error parsing API response: " + e.getMessage());
                    return -1; // Set an appropriate default value for error handling
                }
            }
            private void updateStatusInDatabase(String phoneNumber, int status) {
            	java.sql.Connection conn = null; // Specify the java.sql.Connection class
                PreparedStatement stmt = null;

                try {
                    // Create a database connection
                    conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);

                    // Prepare the SQL update statement
                    String updateSql = "UPDATE inv_sms_dtl SET sms_status_response = ?, send_status = ? WHERE mobileno = ?";
                    stmt = conn.prepareStatement(updateSql);

                    // Set the status and phone number as parameters in the update statement
                    stmt.setInt(1, status);
                    stmt.setInt(2, status);  // Use the same status value for both columns
                    stmt.setString(3, phoneNumber);
                    
                 // Print the phone number and status just before executing the update query
//                    System.out.println("Updating status for Phone Number: " + phoneNumber);
//                    System.out.println("New Status: " + status);

                    // Execute the update query
                    int rowsAffected = stmt.executeUpdate();

                    // Optionally, you can check the number of rows affected to ensure the update was successful
                    if (rowsAffected > 0) {
                        System.out.println("Status updated in the database for Phone Number: " + phoneNumber+"\n ................................ \n");
                    } else {
                        System.out.println("Status update failed for Phone Number: " + phoneNumber+"\n ................................ \n");
                    }
                } catch (SQLException e) {
                    System.out.println("Error updating database: " + e.getMessage());
                } finally {
                    // Close the database resources in the finally block
                    try {
                        if (stmt != null) {
                            stmt.close();
                        }
                        if (conn != null) {
                            conn.close();
                        }
                    } catch (SQLException e) {
                        System.out.println("Error closing database resources: " + e.getMessage());
                    }
                }
            }

            private String getStringHeaderValue(AMQP.BasicProperties properties, String headerName) {
                if (properties != null && properties.getHeaders() != null) {
                    Object headerValue = properties.getHeaders().get(headerName);
                    return (headerValue != null) ? String.valueOf(headerValue) : null;
                }
                return null;
            }
            
            // Method to send FCM notification
            private void sendFcmNotification(String fcmToken) {
            	
                OkHttpClient fcmClient = new OkHttpClient();
                
             // Create the FCM notification payload
                Gson gson = new Gson();
                FcmNotificationPayload notificationPayload = new FcmNotificationPayload();
                notificationPayload.setTo(fcmToken);
                notificationPayload.setData(new FcmNotificationData("/LabReport/", "Investigation report"));
                notificationPayload.setNotification(new FcmNotification(" View Lab Report", " Your Lab Report Has been Generated!"));

                // Convert the notification payload to JSON
                String payloadJson = gson.toJson(notificationPayload);
                
                int maxRetries = 3;
                int retryCount = 0;
                int retryIntervalMs = 2000;
                while (retryCount < maxRetries) {
             // Create the FCM request
                Request fcmRequest = new Request.Builder()
                        .url(FCM_API_URL)
                        .header("Authorization", "key=" + FCM_AUTH_KEY)
                        .header("Content-Type", "application/json")
                        .post(RequestBody.create(MediaType.parse("application/json"), payloadJson))
                        .build();
                
             // Send the FCM request
                try (Response fcmResponse = fcmClient.newCall(fcmRequest).execute()) {
                    if (fcmResponse.isSuccessful()) {
                    	String responseBody = fcmResponse.body().string();
                        System.out.println("FCM response: " + responseBody+"\n - \n");
                     // Parse the FCM response to check the success field
                        JSONObject jsonObject = new JSONObject(responseBody);
                        int success = jsonObject.getInt("success");
                        if (success == 1) {
                            System.out.println("FCM notification sent successfully to token: " + fcmToken+"\n ................................ \n");
                            return; // Notification sent successfully, exit the method
                        } else {
                            // The notification failed, retry if within the max retry count
                            System.out.println("Failed to send FCM notification to token: " + fcmToken);
                            retryCount++;
                            if (retryCount < maxRetries) {
                                System.out.println("Retrying... (Attempt " + retryCount + ")");
                                try {
                                Thread.sleep(retryIntervalMs); // Add a delay before the next retry
                                } catch (InterruptedException e) {
                                    // The thread was interrupted while sleeping
                                    e.printStackTrace(); // For demonstration purposes; use appropriate logging
                                    Thread.currentThread().interrupt(); // Re-interrupt the thread
                                }
                                retryIntervalMs *= 2; // Exponential backoff: double the interval for the next retry
                            }
                    }
                  }
                  else {
                        System.out.println("Failed to send FCM notification to token: " + fcmToken+"\n ................................ \n");
                        retryCount++;
                        if (retryCount < maxRetries) {
                            System.out.println("Retrying... (Attempt " + retryCount + ")");
                        }
                    }
                } catch (IOException e) {
                    System.out.println("Error sending FCM notification: " + e.getMessage()+"\n ................................ \n");
                    retryCount++;
                    if (retryCount < maxRetries) {
                        System.out.println("Retrying... (Attempt " + retryCount + ")");
                    }
                }
                catch (JSONException e) {
                    // An error occurred while parsing the response, retry if within the max retry count
                    System.out.println("Error parsing FCM response: " + e.getMessage());
                    retryCount++;
                    if (retryCount < maxRetries) {
                        System.out.println("Retrying... (Attempt " + retryCount + ")");
                    }
                }
            }
                // If the notification failed even after max retries, display an error message
                System.out.println("Failed to send FCM notification after " + maxRetries + " attempts."+"\n ................................ \n");
            }
            
        });
    }
}




