package com.rabbit.workshop.package2;

import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

@Component
public class MessageListener {

    @RabbitListener(queues = "normalQueue")
    public void handleNormalQueueMessage(String message) {
        System.out.println("Received message from normalQueue: " + message);
        throw new RuntimeException("Simulating message rejection");
    }

    @RabbitListener(queues = "dlxQueue")
    public void handleDlxQueueMessage(String message) {
        System.out.println("Received message from dlxQueue: " + message);
    }
}
