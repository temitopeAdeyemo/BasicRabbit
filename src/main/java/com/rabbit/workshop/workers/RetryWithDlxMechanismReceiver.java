package com.rabbit.workshop.workers;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

public class RetryWithDlxMechanismReceiver {
    private final static String QUEUE_NAME = "my_queue";
    public static void main(String[] args) throws Exception {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        Connection connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel();

        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        DeliverCallback deliverCallback = (consumerTag, delivery)->{
            String message = new String(delivery.getBody());

            System.out.println("Received message on main: "+ message + " ");

            var ackMultipleDeliveries = false;

            var requeue = false;

            var msgArr = message.split(":");

            if(Integer.parseInt(msgArr[1]) == 3) channel.basicAck(delivery.getEnvelope().getDeliveryTag(), ackMultipleDeliveries);
            else channel.basicNack(delivery.getEnvelope().getDeliveryTag(), ackMultipleDeliveries, requeue);
        };

        channel.basicConsume(QUEUE_NAME, false, deliverCallback, consumerTag -> { });
    }
}
