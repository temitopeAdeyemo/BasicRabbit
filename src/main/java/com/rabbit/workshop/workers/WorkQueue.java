package com.rabbit.workshop.workers;

import com.rabbitmq.client.*;

import java.nio.charset.StandardCharsets;

public class WorkQueue {
    // Check https://rabbitmq.github.io/rabbitmq-java-client/api/current/com/rabbitmq/client/package-summary.html to learn about package apis
    private final static String QUEUE_NAME = "task_queue";
    public static void main(String[] args)  throws Exception{
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()

        ) {
            channel.confirmSelect();  // Enable Publisher Confirms
            boolean durable = true;
            channel.queueDeclare(QUEUE_NAME, durable, false, false, null);
//            String message = String.join(" ", args);

            var messageProperties = MessageProperties.PERSISTENT_TEXT_PLAIN; // To ensure messages are not lost after rabbitmq crashes or restarts.

            /**
             * Note on message persistence
             * Marking messages as persistent doesn't fully guarantee that a message won't be lost.
             * Although it tells RabbitMQ to save the message to disk, there is still a short time window when RabbitMQ has accepted a message and hasn't saved it yet.
             * Also, RabbitMQ doesn't do fsync(2) for every message -- it may be just saved to cache and not really written to the disk. The persistence guarantees aren't strong, but it's more than enough for our simple task queue.
             * If you need a stronger guarantee then you can use publisher confirms.
             */
            var exchange = "";
            channel.basicPublish(exchange, QUEUE_NAME, messageProperties, "1 hello.".getBytes()); // The routing key is the queue name for default or nameless exchange i.e ""
            channel.basicPublish(exchange, QUEUE_NAME, messageProperties, "2 hello..".getBytes()); /// The routing key is the queue name for default or nameless exchange i.e ""
            channel.basicPublish(exchange, QUEUE_NAME, messageProperties, "3 hello...".getBytes());
            channel.basicPublish(exchange, QUEUE_NAME, messageProperties, "4 hello....".getBytes());
            channel.basicPublish(exchange, QUEUE_NAME, messageProperties, "5 hello.....".getBytes());
            channel.basicPublish(exchange, QUEUE_NAME, messageProperties, "6 hello......".getBytes());
//            Create new scratch file from selection
            System.out.println(" [x] Sent '" + "hello" + "'");

            channel.addConfirmListener(new ConfirmListener() {
                @Override
                public void handleAck(long deliveryTag, boolean multiple) {
                    // Handle positive acknowledgment
                    System.out.println("Message with tag " + deliveryTag + " has been acknowledged.");
                }

                @Override
                public void handleNack(long deliveryTag, boolean multiple) {
                    // Handle negative acknowledgment
                    System.out.println("Message with tag " + deliveryTag + " has been negatively acknowledged.");
                }

            });
        }
    }
}
