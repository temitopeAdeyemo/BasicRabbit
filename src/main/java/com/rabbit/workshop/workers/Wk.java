package com.rabbit.workshop.workers;

import com.rabbitmq.client.*;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Service
public class Wk {
    private final static String QUEUE_NAME = "task_queue";

    public static void init() throws Exception {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        try(        Connection connection = connectionFactory.newConnection();
                    Channel channel = connection.createChannel()) {
            boolean durable = true; // This makes sure that the queue will survive a RabbitMQ node restart. This marks queue as durable so When RabbitMQ quits or crashes it will forget the queues.

            Map<String, Object> args = new HashMap<>();
//            args.put("x-message-ttl", 60000);

            channel.queueDeclare(QUEUE_NAME, durable, false, false, args); // We declare queue here incase the receiver/consumer is started before the publisher (Where the queue is declared) to avoid trying to read from a non-existence queue..
            System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

            int prefetchCount = 1;

            channel.basicQos(prefetchCount); // This makes the workers accept only one unack-ed message at a time. SO the handle only one message at a time and will not take another one if the initial one is not completed.

            /**
             * Note about queue size
             * If all the workers are busy, your queue can fill up.
             * You will want to keep an eye on that, and maybe add more workers, or have some other strategy.
             */

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody());
                System.out.println("Received message: " + message + " ");
                try {
                    doWork(message);
                } catch (Exception e) {
                    System.out.println(" [x] Error");

                    /**
                     * channel.basicReject is used to reject a message if a worker cannot do the task at the time. requeue can be set to true or false.
                     * False means the message will be routed to a Dead Letter Exchange if it is configured, otherwise it will be discarded.
                     * channel.basicNack can reject multiple messages and requeue them at once but channel.basicReject can only requeue not reject multiple messages at once.
                     *
                     * Requeued messages may be immediately ready for redelivery depending on their position in the queue and the prefetch value used by the channels with active consumers.
                     * This means that if all consumers requeue because they cannot process a delivery due to a transient condition, they will create a requeue/redelivery loop.
                     * Such loops can be costly in terms of network bandwidth and CPU resources.
                     * Consumer implementations can track the number of redeliveries and reject messages for good (discard them) or schedule requeueing after a delay
                     */

                    /**
                     * channel.basicNack is used for negative acknowledgements (note: this is a RabbitMQ extension to AMQP 0-9-1)
                     * channel.basicReject is used for negative acknowledgements but has one limitation compared to basic.nack.
                     *
                     * Positive acknowledgements simply instruct RabbitMQ to record a message as delivered and can be discarded.
                     * Negative acknowledgements with basic.reject have the same effect.
                     * The difference is primarily in the semantics:
                     * positive acknowledgements assume a message was successfully processed while their negative counterpart suggests that a delivery wasn't processed but still should be deleted.
                     */

                    long deliveryTag = delivery.getEnvelope().getDeliveryTag();
                    /**
                     * Set ackMultipleDeliveries to true if channel.basicQos() takes more than 1, i,e worker takes more than one task per delivery.
                     * Though the items in each set of deliveries will have the unique tag, Each message is assigned its own delivery tag when RabbitMQ sends it to a consumer, regardless of how many messages are delivered at a time.
                     * When ackMultipleDeliveries is true, acknowledgement is done after all the task in the set of deliverables is completed, so ack is done for all the task at once.
                     */
                    var ackMultipleDeliveries = false;

                    var requeue = false;
//                  channel.basicReject(deliveryTag, requeue);
                    channel.basicNack(deliveryTag, ackMultipleDeliveries, requeue);
                } finally {
                    var ackMultipleDeliveries = false;
                    channel.basicAck(delivery.getEnvelope().getDeliveryTag(), ackMultipleDeliveries);
                    System.out.println("Done");
                }


            };
            //Meanwhile delivery acknowledgement timeout can be changed.
//        boolean autoAck = true; // This is for auto acknowledgements. Unsafe and This mode is often referred to as "fire-and-forget since acknowledgement is not dependent on success of the task". This means that immediately receiving the task, delivery is sent (irrespective of status of the task as started, completed or failed) and more task can come in, hebce leading to worker overloded with tasks.
            boolean autoAck = false;


            //        channel.basicConsume(QUEUE_NAME, autoAck, deliverCallback, consumerTag -> System.out.println("consumerTag: "+consumerTag)); // Use this or channel.basicConsume below

            channel.basicConsume(QUEUE_NAME, autoAck, "a-consumer-tag",
                    new DefaultConsumer(channel) {
                        @Override
                        public void handleDelivery(String consumerTag,
                                                   Envelope envelope,
                                                   AMQP.BasicProperties properties,
                                                   byte[] body)
                                throws IOException {
                            long deliveryTag = envelope.getDeliveryTag();
                            String message = new String(body);
                            // positively acknowledge a single delivery, the message will
                            // be discarded

                            /**
                             * Set ackMultipleDeliveries to true if channel.basicQos() takes more than 1, i,e worker takes more than one task per delivery.
                             * Though the items in each set of deliveries will have the unique tag, Each message is assigned its own delivery tag when RabbitMQ sends it to a consumer, regardless of how many messages are delivered at a time.
                             * When ackMultipleDeliveries is true, acknowledgement is done after all the task in the set of deliverables is completed, so ack is done for all the task at once.
                             */

                            var ackMultipleDeliveries = false;
                            try{
                                // Do work
                                doWork(message);
                            } catch(Exception e) {
                                /**
                                 * channel.basicReject is used to reject a message if a worker cannot do the task at the time. requeue can be set to true or false.
                                 * False means the message will be routed to a Dead Letter Exchange if it is configured, otherwise it will be discarded.
                                 * channel.basicNack can reject multiple messages and requeue them at once but channel.basicReject can only requeue not reject multiple messages at once.
                                 *
                                 * Requeued messages may be immediately ready for redelivery depending on their position in the queue and the prefetch value used by the channels with active consumers.
                                 * This means that if all consumers requeue because they cannot process a delivery due to a transient condition, they will create a requeue/redelivery loop.
                                 * Such loops can be costly in terms of network bandwidth and CPU resources.
                                 * Consumer implementations can track the number of redeliveries and reject messages for good (discard them) or schedule requeueing after a delay
                                 */

                                var requeue = false;
                                //  channel.basicReject(deliveryTag, requeue);
                                channel.basicNack(deliveryTag, ackMultipleDeliveries, requeue);
                            } finally {
                                channel.basicAck(deliveryTag, ackMultipleDeliveries);
                            }
                        }
                    });

        }
    }

    private static void doWork(String task) throws InterruptedException {
        for (char ch: task.toCharArray()) {
            if (ch == '.') Thread.sleep(1000);
        }
    }
}

