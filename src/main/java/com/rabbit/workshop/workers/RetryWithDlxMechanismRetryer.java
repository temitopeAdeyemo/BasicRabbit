package com.rabbit.workshop.workers;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

public class RetryWithDlxMechanismRetryer {
    public static final String RETRY_QUEUE = "retryQueue";
    public static void main(String[] args) throws Exception {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        Connection connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel();

        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        DeliverCallback deliverCallback = (consumerTag, delivery)->{
            String message = new String(delivery.getBody());

            System.out.println("Received message: "+ message + " ");
            try {
                Thread.sleep(15000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            var ackMultipleDeliveries = false;

            var msgArr = message.split(":");

            msgArr[1] = String.valueOf(Integer.parseInt(msgArr[1])+1);

            message = String.join(":", msgArr);

            channel.basicPublish("MAIN X", "key", null, message.getBytes());

            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), ackMultipleDeliveries);
        };

        channel.basicConsume(RETRY_QUEUE, false, deliverCallback, consumerTag -> { });
    }
}
