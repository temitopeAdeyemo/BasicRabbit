package com.rabbit.workshop;


import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.nio.charset.StandardCharsets;

//@SpringBootApplication
public class RabbitWorkshopApplication {
	private final static String QUEUE_NAME = "hello";

	public static void main(String[] args)  throws Exception{
//		SpringApplication.run(RabbitWorkshopApplication.class, args);
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");

		try (Connection connection = factory.newConnection();
			 Channel channel = connection.createChannel()) {
			channel.queueDeclare(QUEUE_NAME, false, false, false, null);
			String message = "Hello World!";
			channel.basicPublish("", QUEUE_NAME, null, message.getBytes(StandardCharsets.UTF_8)); // The routing key is the queue name for default or nameless exchange i.e ""
			System.out.println(" [x] Sent '" + message + "'");
		}
	}

}
