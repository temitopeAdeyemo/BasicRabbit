package com.rabbit.workshop.Exchange;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.nio.charset.StandardCharsets;

public class EmitLogDirect {
    private static final String EXCHANGE_NAME = "direct_logs";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            /**
             * DIRECT EXCHANGE
             * Routes messages to queues based on an exact match of the routing key and binding key.
             * Uses routing key to determine which queue(s) should receive the message.
             * Use case: Targeted delivery, where a message should only go to specific queues that match the given routing key
             */
            channel.exchangeDeclare(EXCHANGE_NAME, "direct");

            String routingKey = "severe"; // The routing key on the emitter/producer must be the same as the binding key in the consumer/receiver for the consumer to get the message.
            String message = "This is severe.";

            channel.basicPublish(EXCHANGE_NAME, routingKey, null, message.getBytes(StandardCharsets.UTF_8));
            System.out.println(" [x] Sent '" + routingKey + "':'" + message + "'");
        }
    }
    //..
}