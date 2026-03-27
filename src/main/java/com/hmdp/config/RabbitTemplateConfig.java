package com.hmdp.config;

import com.hmdp.service.ISeckillOutboxService;
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
                outboxService.markConfirmed(outboxId);
            } else {
                outboxService.markPublishFailed(outboxId, "confirm_nack:" + cause, extractRetryCount(correlationData));
                log.error("mq confirm nack, outboxId={}, cause={}", outboxId, cause);
            }
        };
    }

    @Bean
    public RabbitTemplate.ReturnsCallback seckillReturnsCallback(ISeckillOutboxService outboxService) {
        return returnedMessage -> {
            Message message = returnedMessage.getMessage();
            String outboxIdHeader = message.getMessageProperties().getHeader("x-outbox-id");
            if (outboxIdHeader == null) {
                return;
            }
            Long outboxId = Long.valueOf(outboxIdHeader);
            outboxService.markPublishFailed(outboxId,
                    "returned:" + returnedMessage.getReplyCode() + ":" + returnedMessage.getReplyText(),
                    extractRetryCount(message));
            log.error("mq returned, outboxId={}, replyCode={}, replyText={}",
                    outboxId, returnedMessage.getReplyCode(), returnedMessage.getReplyText());
        };
    }

    @Bean
    public RabbitTemplate rabbitTemplate(org.springframework.amqp.rabbit.connection.ConnectionFactory connectionFactory,
                                         RabbitTemplate.ConfirmCallback seckillConfirmCallback,
                                         RabbitTemplate.ReturnsCallback seckillReturnsCallback) {
        RabbitTemplate rabbitTemplate = new RabbitTemplate(connectionFactory);
        rabbitTemplate.setMandatory(true);
        rabbitTemplate.setConfirmCallback(seckillConfirmCallback);
        rabbitTemplate.setReturnsCallback(seckillReturnsCallback);
        return rabbitTemplate;
    }

    private int extractRetryCount(CorrelationData correlationData) {
        if (!(correlationData instanceof SeckillCorrelationData)) {
            return 0;
        }
        return ((SeckillCorrelationData) correlationData).getRetryCount();
    }

    private int extractRetryCount(Message message) {
        Integer retryCount = message.getMessageProperties().getHeader("x-retry-count");
        return retryCount == null ? 0 : retryCount;
    }

    public static class SeckillCorrelationData extends CorrelationData {
        private final int retryCount;

        public SeckillCorrelationData(String id, int retryCount) {
            super(id);
            this.retryCount = retryCount;
        }

        public int getRetryCount() {
            return retryCount;
        }
    }
}
