package com.hmdp.mq;

import cn.hutool.json.JSONUtil;
import com.hmdp.dto.SeckillOrderMessage;
import com.hmdp.entity.VoucherOrderOutbox;
import com.hmdp.service.ISeckillOutboxService;
import com.hmdp.service.impl.VoucherOrderServiceImpl;
import com.hmdp.utils.SeckillMqConstants;
import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

@Slf4j
@Component
public class SeckillOrderDlqConsumer {

    @Resource
    private RabbitTemplate rabbitTemplate;

    @Resource
    private VoucherOrderServiceImpl voucherOrderService;

    @Resource
    private ISeckillOutboxService outboxService;

    @Value("${hmdp.seckill.dlq.max-replay:3}")
    private int maxReplay;

    @RabbitListener(queues = SeckillMqConstants.QUEUE_ORDER_DLQ)
    public void onDeadLetterMessage(Message message, Channel channel) throws IOException {
        long deliveryTag = message.getMessageProperties().getDeliveryTag();
        String payload = new String(message.getBody(), StandardCharsets.UTF_8);
        SeckillOrderMessage orderMessage;
        try {
            orderMessage = JSONUtil.toBean(payload, SeckillOrderMessage.class);
            if (orderMessage.getOutboxId() == null
                    || orderMessage.getOrderId() == null
                    || orderMessage.getUserId() == null
                    || orderMessage.getVoucherId() == null) {
                throw new IllegalArgumentException("seckill dlq message required fields missing");
            }
        } catch (Exception e) {
            log.error("invalid dlq message payload, payload={}", payload, e);
            channel.basicAck(deliveryTag, false);
            return;
        }
        try {
            VoucherOrderOutbox outbox = outboxService.getById(orderMessage.getOutboxId());
            if (outbox == null) {
                channel.basicAck(deliveryTag, false);
                return;
            }
            if (outbox.getStatus() == null || outbox.getStatus() != ISeckillOutboxService.STATUS_DELIVERED) {
                channel.basicAck(deliveryTag, false);
                return;
            }
            Integer retryCount = outbox.getRetryCount();
            int replayCount = retryCount == null ? 0 : retryCount;
            if (replayCount < maxReplay) {
                Message replayMessage = MessageBuilder.withBody(message.getBody())
                        .setContentType(message.getMessageProperties().getContentType() == null
                                ? MessageProperties.CONTENT_TYPE_JSON
                                : message.getMessageProperties().getContentType())
                        .setDeliveryMode(MessageDeliveryMode.PERSISTENT)
                        .build();
                rabbitTemplate.send(
                        SeckillMqConstants.EXCHANGE_ORDER,
                        SeckillMqConstants.ROUTING_KEY_ORDER_CREATED,
                        replayMessage
                );
                boolean recorded = outboxService.recordDlqReplay(orderMessage.getOutboxId(), replayCount);
                if (!recorded) {
                    log.warn("record dlq replay count skipped, outboxId={}, replay={}",
                            orderMessage.getOutboxId(), replayCount + 1);
                }
                channel.basicAck(deliveryTag, false);
                log.warn("replay dlq message to order queue, outboxId={}, replay={}",
                        orderMessage.getOutboxId(), replayCount + 1);
                return;
            }

            boolean compensated = voucherOrderService.compensateReservation(
                    orderMessage.getVoucherId(),
                    orderMessage.getUserId()
            );
            String reason = compensated
                    ? "dlq_replay_exhausted_compensated"
                    : "dlq_replay_exhausted_compensate_noop";
            if (compensated) {
                outboxService.removeById(orderMessage.getOutboxId());
            } else {
                outboxService.markDead(orderMessage.getOutboxId(), reason);
            }
            channel.basicAck(deliveryTag, false);
            log.error("dlq replay exhausted, apply compensation, outboxId={}, result={}",
                    orderMessage.getOutboxId(), compensated);
        } catch (Exception e) {
            log.error("handle dlq message failed, payload={}", payload, e);
            channel.basicNack(deliveryTag, false, true);
        }
    }
}
