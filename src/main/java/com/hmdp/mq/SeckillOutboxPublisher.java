package com.hmdp.mq;

import com.hmdp.dto.SeckillCorrelationData;
import com.hmdp.entity.VoucherOrderOutbox;
import com.hmdp.service.ISeckillOutboxService;
import com.hmdp.utils.SeckillMqConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;

@Slf4j
@Component
public class SeckillOutboxPublisher {

    @Resource
    private ISeckillOutboxService seckillOutboxService;

    @Resource
    private RabbitTemplate rabbitTemplate;

    @Value("${hmdp.seckill.outbox.publish-batch-size:100}")
    private int publishBatchSize;

    @Scheduled(fixedDelayString = "${hmdp.seckill.outbox.publish-delay-ms:200}")
    public void publishOutboxMessages() {
        List<VoucherOrderOutbox> candidates = seckillOutboxService.findPublishCandidates(publishBatchSize);
        if (candidates.isEmpty()) {
            return;
        }
        for (VoucherOrderOutbox outbox : candidates) {
            publishSingleOutbox(outbox);
        }
    }

    private void publishSingleOutbox(VoucherOrderOutbox outbox) {
        if (outbox == null || outbox.getId() == null || outbox.getPayload() == null) {
            return;
        }
        int retryCount = outbox.getRetryCount() == null ? 0 : outbox.getRetryCount();
        boolean claimed = seckillOutboxService.markSending(outbox.getId(), retryCount);
        if (!claimed) {
            return;
        }
        try {
            rabbitTemplate.convertAndSend(
                    SeckillMqConstants.EXCHANGE_ORDER,
                    SeckillMqConstants.ROUTING_KEY_ORDER_CREATED,
                    outbox.getPayload(),
                    message -> {
                        message.getMessageProperties().setContentType(MessageProperties.CONTENT_TYPE_JSON);
                        message.getMessageProperties().setDeliveryMode(MessageDeliveryMode.PERSISTENT);
                        message.getMessageProperties().setHeader(SeckillMqConstants.HEADER_OUTBOX_ID, outbox.getId().toString());
                        message.getMessageProperties().setHeader(SeckillMqConstants.HEADER_PUBLISH_RETRY_COUNT, retryCount);
                        return message;
                    },
                    new SeckillCorrelationData(String.valueOf(outbox.getId()), retryCount)
            );
        } catch (Exception e) {
            seckillOutboxService.markPublishFailed(outbox.getId(), "publish_exception:" + e.getMessage(), retryCount);
            log.error("publish outbox to rabbitmq failed, outboxId={}, retry={}", outbox.getId(), retryCount, e);
        }
    }
}
