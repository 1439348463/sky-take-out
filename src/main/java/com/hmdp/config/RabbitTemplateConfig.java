package com.hmdp.config;

import com.hmdp.dto.SeckillCorrelationData;
import com.hmdp.service.ISeckillOutboxService;
import com.hmdp.utils.SeckillMqConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class RabbitTemplateConfig {

    @Bean
    public RabbitTemplate.ConfirmCallback seckillConfirmCallback(ISeckillOutboxService outboxService) {
        return (CorrelationData correlationData, boolean ack, String cause) -> {
            if (correlationData == null || correlationData.getId() == null) {
                return;
            }
            Long outboxId = Long.valueOf(correlationData.getId());
            if (ack) {
                outboxService.markDelivered(outboxId, extractRetryCount(correlationData));
            } else {
                outboxService.markPublishFailed(outboxId, "confirm_nack:" + cause, extractRetryCount(correlationData));
                log.error("mq confirm nack, outboxId={}, cause={}", outboxId, cause);
            }
        };
    }

    @Bean
    public RabbitTemplate.ReturnCallback seckillReturnCallback(ISeckillOutboxService outboxService) {
        return (Message message, int replyCode, String replyText, String exchange, String routingKey) -> {
            Long outboxIdHeader = extractOutboxId(message);
            if (outboxIdHeader == null) {
                return;
            }
            outboxService.markReturned(outboxIdHeader,
                    "returned:" + replyCode + ":" + replyText,
                    extractRetryCount(message));
            log.error("mq returned, outboxId={}, replyCode={}, replyText={}",
                    outboxIdHeader, replyCode, replyText);
        };
    }

    @Bean
    public RabbitTemplate rabbitTemplate(org.springframework.amqp.rabbit.connection.ConnectionFactory connectionFactory,
                                         RabbitTemplate.ConfirmCallback seckillConfirmCallback,
                                         RabbitTemplate.ReturnCallback seckillReturnCallback) {
        RabbitTemplate rabbitTemplate = new RabbitTemplate(connectionFactory);
        rabbitTemplate.setMandatory(true);
        rabbitTemplate.setConfirmCallback(seckillConfirmCallback);
        rabbitTemplate.setReturnCallback(seckillReturnCallback);
        return rabbitTemplate;
    }

    private int extractRetryCount(CorrelationData correlationData) {
        if (!(correlationData instanceof SeckillCorrelationData)) {
            return 0;
        }
        return ((SeckillCorrelationData) correlationData).getRetryCount();
    }

    private int extractRetryCount(Message message) {
        Object header = message.getMessageProperties().getHeaders().get(SeckillMqConstants.HEADER_PUBLISH_RETRY_COUNT);
        if (header instanceof Number) {
            return ((Number) header).intValue();
        }
        if (header instanceof String) {
            try {
                return Integer.parseInt((String) header);
            } catch (NumberFormatException e) {
                log.warn("invalid publish retry header: {}", header);
            }
        }
        return 0;
    }

    private Long extractOutboxId(Message message) {
        Object header = message.getMessageProperties().getHeaders().get(SeckillMqConstants.HEADER_OUTBOX_ID);
        if (header instanceof Number) {
            return ((Number) header).longValue();
        }
        if (header instanceof String) {
            try {
                return Long.valueOf((String) header);
            } catch (NumberFormatException e) {
                log.warn("invalid outbox id header: {}", header);
            }
        }
        return null;
    }
}
