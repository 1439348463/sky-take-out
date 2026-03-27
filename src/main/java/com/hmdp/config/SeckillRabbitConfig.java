package com.hmdp.config;

import com.hmdp.utils.SeckillMqConstants;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SeckillRabbitConfig {

    @Bean
    public DirectExchange seckillOrderExchange() {
        return new DirectExchange(SeckillMqConstants.EXCHANGE_ORDER, true, false);
    }

    @Bean
    public DirectExchange seckillOrderDlxExchange() {
        return new DirectExchange(SeckillMqConstants.EXCHANGE_ORDER_DLX, true, false);
    }

    @Bean
    public Queue seckillOrderQueue() {
        return QueueBuilder.durable(SeckillMqConstants.QUEUE_ORDER)
                .deadLetterExchange(SeckillMqConstants.EXCHANGE_ORDER_DLX)
                .deadLetterRoutingKey(SeckillMqConstants.ROUTING_KEY_ORDER_DLX)
                .build();
    }

    @Bean
    public Queue seckillOrderDlq() {
        return QueueBuilder.durable(SeckillMqConstants.QUEUE_ORDER_DLQ).build();
    }

    @Bean
    public Binding seckillOrderBinding(Queue seckillOrderQueue, DirectExchange seckillOrderExchange) {
        return BindingBuilder.bind(seckillOrderQueue).to(seckillOrderExchange).with(SeckillMqConstants.ROUTING_KEY_ORDER_CREATED);
    }

    @Bean
    public Binding seckillOrderDlqBinding(Queue seckillOrderDlq, DirectExchange seckillOrderDlxExchange) {
        return BindingBuilder.bind(seckillOrderDlq).to(seckillOrderDlxExchange).with(SeckillMqConstants.ROUTING_KEY_ORDER_DLX);
    }
}
