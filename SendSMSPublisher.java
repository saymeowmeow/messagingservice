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
        factory.setHost("");
        factory.setPort(5672);
        factory.setUsername("");
        factory.setPassword("");

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
                    rs = sd.getResult("select * from column where send_status=0 ");

                    while (rs.next()) {
                        OkHttpClient client = new OkHttpClient().newBuilder().build();
 // Construct the form data
                        RequestBody requestBody = new FormBody.Builder()
                                .add("template_id", "1007755918135810961")
            .add("sms_content", "")
            .add("phonenumber", rs.getString(3))
            .add("department_id", "D006003")
            .add("action", "singlewithbulkunicodeSMS")
            .build();

                     // Construct the headers map
                        Map<String, Object> headers = new HashMap<String, Object>();
                        headers.put("template_id", "1007755918135810961");
                        headers.put("sms_content", "");
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
