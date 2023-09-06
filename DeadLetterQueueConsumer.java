import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class DeadLetterQueueConsumer {
    private static final String DLQ_NAME = "sms_queue_dlq";

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("10.226.30.117");
        factory.setPort(5672);
        factory.setUsername("newadmin");
        factory.setPassword("Cdac@123");

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        channel.queueDeclare(DLQ_NAME, false, false, false, null);

        System.out.println("Waiting for messages from Dead Letter Queue... \n");

        channel.basicConsume(DLQ_NAME, false, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, StandardCharsets.UTF_8);
                
                // Your processing logic for messages from the dead-letter queue
                System.out.println("Received message from Dead Letter Queue: " + message);

                // Acknowledge the message to remove it from the dead-letter queue
                channel.basicAck(envelope.getDeliveryTag(), false);
            }
        });
    }
}
