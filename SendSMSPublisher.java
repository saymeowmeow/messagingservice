import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;

import okhttp3.FormBody;
import okhttp3.MediaType;
import okhttp3.MultipartBody;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okio.Buffer;

public class SendSMSPublisher {
    private static final String QUEUE_NAME = "sms_queue";

    public static void main(String[] args) {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("10.226.30.117");
        factory.setPort(5672);
        factory.setUsername("newadmin");
        factory.setPassword("Cdac@123");

        Connection connection = null;
        Channel channel = null;

        try {
            connection = factory.newConnection();
            channel = connection.createChannel();
            channel.queueDeclare(QUEUE_NAME, false, false, false, null);

            ResultSet rs = null;
            Select_Data sd = new Select_Data();
//      InsertData ins = new InsertData();

            while (true) {
                try {
                    rs = sd.getResult("select * from inv_sms_dtl where send_status=0 ");
                    

//          Statement stmt = ins.getCon().createStatement();
//          String sql = "update inv_sms_dtl as inv set inv.send_status=1 where  inv.cr_no in (select sms.cr_no from inv_sms_dtl as sms where sms.send_status=0)";
//          stmt.executeUpdate(sql);

//          while (rs.next()) {
//            OkHttpClient client = new OkHttpClient().newBuilder().build();
//            MediaType mediaType = MediaType.parse("text/plain");
//            RequestBody body = new MultipartBody.Builder().setType(MultipartBody.FORM)
//                .addFormDataPart("template_id","1007755918135810961")
////                .addFormDataPart("sms_content","à¬ªà­�à¬°à¬¿à­Ÿ "+ rs.getString(2).trim() +", CRNO: " + rs.getLong(1) + ", à¬†à¬ªà¬£à¬™à­�à¬•à¬°  " + rs.getString(4) + " à¬°à¬¿à¬ªà­‹à¬°à­�à¬Ÿ à¬ªà­�à¬°à¬¸à­�à¬¤à­�à¬¤ à¬¹à­‹à¬‡à¬¯à¬¾à¬‡à¬…à¬›à¬¿, à¬¦à­Ÿà¬¾à¬•à¬°à¬¿ à¬†à¬ªà¬£à¬™à­�à¬•à¬° à¬¸à­�à¬¬à¬¿à¬§à¬¾ à¬…à¬¨à­�à¬¸à¬¾à¬°à­‡ à¬•à¬¾à¬‰à¬£à­�à¬Ÿà¬° à¬°à­� à¬¸à¬‚à¬—à­�à¬°à¬¹ à¬•à¬°à¬¨à­�à¬¤à­� à¬•à¬¿à¬®à­�à¬¬à¬¾ https://eswasthya.odisha.gov.in/viewlabreports.html à¬°à­� à¬¡à¬¾à¬‰à¬¨à¬²à­‹à¬¡ à¬•à¬°à¬¨à­�à¬¤à­� à¥¤ à¬¸à­�à­±à¬¾à¬¸à­�à¬¥à­�à­Ÿ à¬“ à¬ªà¬°à¬¿à¬¬à¬¾à¬° à¬•à¬²à­�à­Ÿà¬¾à¬£ à¬¬à¬¿à¬­à¬¾à¬—, à¬“à¬¡à¬¿à¬¶à¬¾ à¬¸à¬°à¬•à¬¾à¬°à¥¤")
////                .addFormDataPart("sms_content","ପ୍ରିୟ Jatin, CRNO: 211012300019778, ଆପଣଙ୍କର test ରିପୋର୍ଟ ପ୍ରସ୍ତୁତ ହୋଇଯାଇଅଛି, ଦୟାକରି ଆପଣଙ୍କର ସୁବିଧା ଅନୁସାରେ କାଉଣ୍ଟର ରୁ ସଂଗ୍ରହ କରନ୍ତୁ କିମ୍ବା https://eswasthya.odisha.gov.in/viewlabreports.html ରୁ ଡାଉନଲୋଡ କରନ୍ତୁ । ସ୍ୱାସ୍ଥ୍ୟ ଓ ପରିବାର କଲ୍ୟାଣ ବିଭାଗ, ଓଡିଶା ସରକାର।")
//                .addFormDataPart("sms_content","ପ୍ରିୟ " + rs.getString(2).trim() +" , CRNO: " + rs.getLong(1) + ", ଆପଣଙ୍କର test ରିପୋର୍ଟ ପ୍ରସ୍ତୁତ ହୋଇଯାଇଅଛି, ଦୟାକରି ଆପଣଙ୍କର ସୁବିଧା ଅନୁସାରେ କାଉଣ୍ଟର ରୁ ସଂଗ୍ରହ କରନ୍ତୁ କିମ୍ବା https://eswasthya.odisha.gov.in/viewlabreports.html ରୁ ଡାଉନଲୋଡ କରନ୍ତୁ । ସ୍ୱାସ୍ଥ୍ୟ ଓ ପରିବାର କଲ୍ୟାଣ ବିଭାଗ, ଓଡିଶା ସରକାର।")
//                .addFormDataPart("phonenumber", rs.getString(3))
//                .addFormDataPart("department_id","D006003")
//                .addFormDataPart("action","singlewithbulkunicodeSMS")
//                .build();
//          }

                    while (rs.next()) {
                        OkHttpClient client = new OkHttpClient().newBuilder().build();
 // Construct the form data
                        RequestBody requestBody = new FormBody.Builder()
                                .add("template_id", "1007755918135810961")
            .add("sms_content", "ପ୍ରିୟ " + rs.getString(2).trim() + " , CRNO: " + rs.getLong(1) + ", ଆପଣଙ୍କର test ରିପୋର୍ଟ ପ୍ରସ୍ତୁତ ହୋଇଯାଇଅଛି, ଦୟାକରି ଆପଣଙ୍କର ସୁବିଧା ଅନୁସାରେ କାଉଣ୍ଟର ରୁ ସଂଗ୍ରହ କରନ୍ତୁ କିମ୍ବା https://eswasthya.odisha.gov.in/viewlabreports.html ରୁ ଡାଉନଲୋଡ କରନ୍ତୁ । ସ୍ୱାସ୍ଥ୍ୟ ଓ ପରିବାର କଲ୍ୟାଣ ବିଭାଗ, ଓଡିଶା ସରକାର।")
            .add("phonenumber", rs.getString(3))
            .add("department_id", "D006003")
            .add("action", "singlewithbulkunicodeSMS")
            .build();

                     // Construct the headers map
                        Map<String, Object> headers = new HashMap<String, Object>();
                        headers.put("template_id", "1007755918135810961");
                        headers.put("sms_content", "ପ୍ରିୟ " + rs.getString(2).trim() + " , CRNO: " + rs.getLong(1) + ", ଆପଣଙ୍କର test ରିପୋର୍ଟ ପ୍ରସ୍ତୁତ ହୋଇଯାଇଅଛି, ଦୟାକରି ଆପଣଙ୍କର ସୁବିଧା ଅନୁସାରେ କାଉଣ୍ଟର ରୁ ସଂଗ୍ରହ କରନ୍ତୁ କିମ୍ବା https://eswasthya.odisha.gov.in/viewlabreports.html ରୁ ଡାଉନଲୋଡ କରନ୍ତୁ । ସ୍ୱାସ୍ଥ୍ୟ ଓ ପରିବାର କଲ୍ୟାଣ ବିଭାଗ, ଓଡିଶା ସରକାର।");
                        headers.put("phonenumber", rs.getString(3));
                        headers.put("department_id", "D006003");
                        headers.put("action", "singlewithbulkunicodeSMS");

                        // Enqueue the form data as a serialized FormBody object into RabbitMQ
                        channel.basicPublish("", QUEUE_NAME, new AMQP.BasicProperties.Builder()
                            .headers(headers)
                            .build(), serializeFormBody(requestBody));



                        System.out.println("SMS Enqueued for Mobile No: " + rs.getString(3));
                        System.out.println("");
}

          Thread.sleep(710000L);
        } catch (Exception aa) {
          System.out.println("Error " + aa);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      // Close the resources in the finally block
      try {
        if (channel != null) {
          channel.close();
        }
        if (connection != null) {
          connection.close();
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
    private static byte[] serializeFormBody(RequestBody requestBody) throws IOException {
        Buffer buffer = new Buffer();
        requestBody.writeTo(buffer);
        return buffer.readByteArray();
    }
}

//public class SendSMSPublisher {
//  private static final String QUEUE_NAME = "sms_queue";
//
//  public static void main(String[] args) {
//    ConnectionFactory factory = new ConnectionFactory();
//    factory.setHost("10.226.30.117");
//    factory.setPort(5672);
//    factory.setUsername("admin"); 
//    factory.setPassword("Cdac@123"); 
//
//    Connection connection = null;
//    Channel channel = null;
//
//    try {
//      connection = factory.newConnection();
//      channel = connection.createChannel();
//      channel.queueDeclare(QUEUE_NAME, false, false, false, null);
//
//      ResultSet rs = null;
//      Select_Data sd = new Select_Data();
//      InsertData ins = new InsertData();
//
//      while (true) {
//        try {
//          rs = sd.getResult("select * from inv_sms_dtl where send_status=0 ");
//
//          Statement stmt = ins.getCon().createStatement();
//          String sql = "update inv_sms_dtl as inv set inv.send_status=1 where  inv.cr_no in (select sms.cr_no from inv_sms_dtl as sms where sms.send_status=0)";
//          stmt.executeUpdate(sql);
//
//          while (rs.next()) {
//            OkHttpClient client = new OkHttpClient().newBuilder().build();
//            MediaType mediaType = MediaType.parse("text/plain");
//            RequestBody body = new MultipartBody.Builder().setType(MultipartBody.FORM)
//                .addFormDataPart("template_id","1007755918135810961")
//                .addFormDataPart("sms_content","à¬ªà­�à¬°à¬¿à­Ÿ "+ rs.getString(2).trim() +", CRNO: " + rs.getLong(1) + ", à¬†à¬ªà¬£à¬™à­�à¬•à¬°  " + rs.getString(4) + " à¬°à¬¿à¬ªà­‹à¬°à­�à¬Ÿ à¬ªà­�à¬°à¬¸à­�à¬¤à­�à¬¤ à¬¹à­‹à¬‡à¬¯à¬¾à¬‡à¬…à¬›à¬¿, à¬¦à­Ÿà¬¾à¬•à¬°à¬¿ à¬†à¬ªà¬£à¬™à­�à¬•à¬° à¬¸à­�à¬¬à¬¿à¬§à¬¾ à¬…à¬¨à­�à¬¸à¬¾à¬°à­‡ à¬•à¬¾à¬‰à¬£à­�à¬Ÿà¬° à¬°à­� à¬¸à¬‚à¬—à­�à¬°à¬¹ à¬•à¬°à¬¨à­�à¬¤à­� à¬•à¬¿à¬®à­�à¬¬à¬¾ https://eswasthya.odisha.gov.in/viewlabreports.html à¬°à­� à¬¡à¬¾à¬‰à¬¨à¬²à­‹à¬¡ à¬•à¬°à¬¨à­�à¬¤à­� à¥¤ à¬¸à­�à­±à¬¾à¬¸à­�à¬¥à­�à­Ÿ à¬“ à¬ªà¬°à¬¿à¬¬à¬¾à¬° à¬•à¬²à­�à­Ÿà¬¾à¬£ à¬¬à¬¿à¬­à¬¾à¬—, à¬“à¬¡à¬¿à¬¶à¬¾ à¬¸à¬°à¬•à¬¾à¬°à¥¤")
//                .addFormDataPart("phonenumber", rs.getString(3))
//                .addFormDataPart("department_id","D006003")
//                .addFormDataPart("action","singlewithbulkunicodeSMS")
//                .build();
//
//            Request request = new Request.Builder()
//                .url("https://govtsms.odisha.gov.in/api/api.php?")
//                .method("POST", body)
//                .build();
//
//            // Enqueue the request body into RabbitMQ
//            channel.basicPublish("", QUEUE_NAME, null, request.toString().getBytes());
//
//            System.out.println("SMS Enqueued for Mobile No: " + rs.getString(3));
//            System.out.println("");
//          }
//
//          Thread.sleep(710000L);
//        } catch (Exception aa) {
//          System.out.println("Error " + aa);
//        }
//      }
//    } catch (Exception e) {
//      e.printStackTrace();
//    } finally {
//      // Close the resources in the finally block
//      try {
//        if (channel != null) {
//          channel.close();
//        }
//        if (connection != null) {
//          connection.close();
//        }
//      } catch (Exception e) {
//        e.printStackTrace();
//      }
//    }
//  }
//}
