package com.rabbit.workshop.workers;

import com.rabbitmq.client.*;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class RetryWithDlxMechanism {
    public static final String RETRY_QUEUE = "retryQueue";
    private final static String QUEUE_NAME = "my_queue";
    public static final String MY_DLX = "my-dlx";

    public static void main(String[] args)  throws Exception{
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            Map<String, Object> argss = new HashMap<>();
      	    argss.put("x-dead-letter-exchange", MY_DLX);
            argss.put("x-dead-letter-routing-key", "hello");

            channel.exchangeDeclare(MY_DLX, BuiltinExchangeType.DIRECT);

            channel.queueDeclare(QUEUE_NAME, false, false, false, argss);

            channel.exchangeDeclare("MAIN X", BuiltinExchangeType.DIRECT);

            channel.queueBind(QUEUE_NAME, "MAIN X", "key");

            Map<String, Object> argss2 = new HashMap<>();
            argss2.put("x-message-ttl", 30000);
            argss2.put("x-dead-letter-exchange", "MAIN X");
            argss2.put("x-dead-letter-routing-key", "key");


            channel.queueDeclare(RETRY_QUEUE, true, false, false, argss2);
            channel.queueBind(RETRY_QUEUE, MY_DLX, "hello");

            String message = "Hello tiwi:1";

            AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                    .build();

            channel.basicPublish("", QUEUE_NAME, properties, message.getBytes(StandardCharsets.UTF_8)); // The routing key is the queue name for default or nameless exchange i.e ""
            System.out.println(" [x] Sent '" + message + "'");
        }
    }
}
