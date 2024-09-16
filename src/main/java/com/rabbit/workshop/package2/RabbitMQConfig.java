package com.rabbit.workshop.package2;

import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableRabbit // Set Configurations as done in the application.properties
public class RabbitMQConfig {

    @Bean
    public DirectExchange normalExchange() {
        return new DirectExchange("normalExchange");
    }

    @Bean
    public Queue normalQueue() {
        return QueueBuilder.durable("normalQueue")
                .withArgument("x-dead-letter-exchange", "dlxExchange")
                .withArgument("x-dead-letter-routing-key", "dlxRoutingKey")
                .build();
    }

    @Bean
    public Binding binding() {
        return BindingBuilder.bind(normalQueue())
                .to(normalExchange())
                .with("normalRoutingKey");
    }

    @Bean
    public DirectExchange dlxExchange() {
        return new DirectExchange("dlxExchange");
    }

    @Bean
    public Queue dlxQueue() {
        return new Queue("dlxQueue", true);
    }

    @Bean
    public Binding dlxBinding() {
        return BindingBuilder.bind(dlxQueue())
                .to(dlxExchange())
                .with("dlxRoutingKey");
    }
}
