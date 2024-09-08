package com.rabbit.workshop.Exchange;

import com.rabbitmq.client.*;

public class EmitLog {
    private static final String EXCHANGE_NAME = "logs";

    public static void main(String[] argv) throws Exception {
        /**
         * The core idea in the messaging model in RabbitMQ is that the producer never sends any messages directly to a queue.
         * Actually, quite often the producer doesn't even know if a message will be delivered to any queue at all.
         *
         * Instead, the producer can only send messages to an exchange.
         * An exchange is a very simple thing. On one side it receives messages from producers and the other side it pushes them to queues.
         * The exchange must know exactly what to do with a message it receives. Should it be appended to a particular queue? Should it be appended to many queues? Or should it get discarded.
         * The rules for that are defined by the exchange type.
         */


        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            channel.exchangeDeclare(EXCHANGE_NAME, "fanout");

            /**
             * FANOUT EXCHANGE
             * Broadcasts messages to all bound queues, regardless of the routing key.
             * Routing key is ignored. Any message sent to a fanout exchange is routed to all queues bound to that exchange.
             * Use case: Broadcasting messages to multiple receivers, where the message should be received by all consumers, regardless of criteria.
             */
            String message =  "info: Hello World!" ;

            channel.basicPublish(EXCHANGE_NAME, "", null, message.getBytes("UTF-8"));
            System.out.println(" [x] Sent '" + message + "'");
        }
    }

}
