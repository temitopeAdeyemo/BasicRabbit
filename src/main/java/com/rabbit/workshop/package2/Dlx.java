package com.rabbit.workshop.package2;

import com.rabbitmq.client.*;

import java.util.Map;

public class Dlx {
    private final static String EXCHANGE_NAME = "normalExchange";
    private final static String QUEUE_NAME = "normalQueue";
    private final static String DLX_EXCHANGE_NAME = "dlxExchange";
    private final static String DLX_QUEUE_NAME = "dlxQueue";
    private final static String ROUTING_KEY = "normalRoutingKey";
    private final static String DLX_ROUTING_KEY = "dlxRoutingKey";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setUsername("guest");
        factory.setPassword("guest");

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            // Declare exchanges and queues
            channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);
            channel.queueDeclare(QUEUE_NAME, true, false, false, Map.of(
                    "x-dead-letter-exchange", DLX_EXCHANGE_NAME,
                    "x-dead-letter-routing-key", DLX_ROUTING_KEY,
                    "x-message-ttl", 10000)); // TTL in milliseconds
            channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, ROUTING_KEY);

            channel.exchangeDeclare(DLX_EXCHANGE_NAME, BuiltinExchangeType.DIRECT);
            channel.queueDeclare(DLX_QUEUE_NAME, true, false, false, null);
            channel.queueBind(DLX_QUEUE_NAME, DLX_EXCHANGE_NAME, DLX_ROUTING_KEY);

            String message = "Hello RabbitMQ!";
            channel.basicPublish(EXCHANGE_NAME, ROUTING_KEY, null, message.getBytes());
            System.out.println(" [x] Sent '" + message + "'");

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String receivedMessage = new String(delivery.getBody(), "UTF-8");
                System.out.println(" [x] Received '" + receivedMessage + "'");

                channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, false);
            };

//            channel.basicConsume(QUEUE_NAME, false, deliverCallback, consumerTag -> {});

            DeliverCallback dlxDeliverCallback = (consumerTag, delivery) -> {
                String receivedMessage = new String(delivery.getBody(), "UTF-8");
                System.out.println(" [x] Received from DLX '" + receivedMessage + "'");
            };
//            channel.basicConsume(DLX_QUEUE_NAME, true, dlxDeliverCallback, consumerTag -> {});

            Thread.sleep(40000);
        }
    }
}