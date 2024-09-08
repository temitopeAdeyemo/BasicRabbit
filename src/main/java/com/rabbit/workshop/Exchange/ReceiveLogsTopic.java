package com.rabbit.workshop.Exchange;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.nio.charset.StandardCharsets;

public class ReceiveLogsTopic {
    /**
     * Here’s what the patterns *.orange.* and *.*.mango mean:
     *
     * 1. *.orange.*
     * Pattern Explanation:
     *
     * * (star) matches exactly one word.
     * .orange. is a fixed part of the routing key.
     * The first * matches exactly one word before .orange., and the second * matches exactly one word after .orange..
     * Matches:
     *
     * quick.orange.apple
     * fast.orange.banana
     * red.orange.fruit
     * Does Not Match:
     *
     * orange.apple (missing a word before .orange.)
     * quick.orange.apple.fruit (extra word after .orange.)
     *
     *
     * *.*.mango
     * Pattern Explanation:
     *
     * * (star) matches exactly one word.
     * .mango is a fixed part of the routing key.
     * The pattern *.*.mango expects exactly two words before .mango.
     * Matches:
     *
     * fruit.citrus.mango
     * sweet.fruit.mango
     * green.apple.mango
     * Does Not Match:
     *
     * fruit.mango (missing a word before .mango)
     * fruit.citrus.sweet.mango (extra word before .mango)
     *
     * Pattern Explanation for log.#:
     * Wildcard #: Matches zero or more words.
     * Pattern log.#:
     * log. is a fixed part of the routing key.
     * # matches zero or more words after log..
     * Matching Behavior:
     * Matches:
     *
     * log.info
     * log.error
     * log.server.error
     * log.server.error.details
     * log.warning
     * log.server
     * log (if routing key is exactly log and you have queues bound with this pattern)
     * Does Not Match:
     *
     * info.log (does not start with log.)
     * error.log (does not start with log.)
     */
    private static final String EXCHANGE_NAME = "topic_logs";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME, "topic");
        String queueName = channel.queueDeclare().getQueue();

        var bindingKey = "*.info";
        channel.queueBind(queueName, EXCHANGE_NAME, bindingKey);

        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println(" [x] Received '" +
                    delivery.getEnvelope().getRoutingKey() + "':'" + message + "'");
        };
        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> { });
    }
}
