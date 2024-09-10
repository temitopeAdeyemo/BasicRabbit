package com.rabbit.workshop;


import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

//@SpringBootApplication
public class RabbitWorkshopApplication {
	private final static String QUEUE_NAME = "hello";

	public static void main(String[] args)  throws Exception{
//		SpringApplication.run(RabbitWorkshopApplication.class, args);
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");

		try (Connection connection = factory.newConnection();
			 Channel channel = connection.createChannel()) {
			Map<String, Object> argss = new HashMap<>();
//      	args.put("x-message-ttl", 60000); // Sets time to live for messages in the queue before they expires
//			argss.put("x-expires", 1800000); // This example in Java creates a queue which expires after it has been unused for 30 minutes. If an entire queue expires, the messages in the queue are not dead-lettered.
//			argss.put("x-max-length", 10); // This example in Java declares a queue with a maximum length of 10 messages
			/**
			 * Overflow behaviour can be set by supplying the x-overflow queue declaration argument with a string value.
			 * Possible values are drop-head (default), reject-publish or reject-publish-dlx
			 */
// 			argss.put("x-overflow", "reject-publish");

			channel.queueDeclare(QUEUE_NAME, false, false, false, argss);
			String message = "Hello World!";

			AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
//					.expiration("60000") // Sets per message time to live. Applied Message will expire after ttl.
					.build();

			channel.basicPublish("", QUEUE_NAME, properties, message.getBytes(StandardCharsets.UTF_8)); // The routing key is the queue name for default or nameless exchange i.e ""
			System.out.println(" [x] Sent '" + message + "'");
		}
	}

}
