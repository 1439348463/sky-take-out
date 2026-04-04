package com.hmdp.mq;

import cn.hutool.json.JSONUtil;
import com.hmdp.dto.SeckillOrderMessage;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.metrics.SeckillMetrics;
import com.hmdp.service.ISeckillOutboxService;
import com.hmdp.service.impl.VoucherOrderServiceImpl;
import com.hmdp.utils.SeckillMqConstants;
import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

@Slf4j
@Component
public class SeckillOrderConsumer {

    @Resource
    private VoucherOrderServiceImpl voucherOrderService;

    @Resource
    private SeckillMetrics seckillMetrics;

    @Resource
    private ISeckillOutboxService outboxService;

    @RabbitListener(queues = SeckillMqConstants.QUEUE_ORDER)
    public void onOrderMessage(Message message, Channel channel) throws IOException {
        long deliveryTag = message.getMessageProperties().getDeliveryTag();
        String payload = new String(message.getBody(), StandardCharsets.UTF_8);
        try {
            SeckillOrderMessage orderMessage = JSONUtil.toBean(payload, SeckillOrderMessage.class);
            if (orderMessage.getOrderId() == null || orderMessage.getUserId() == null || orderMessage.getVoucherId() == null) {
                throw new IllegalArgumentException("seckill order message required fields missing");
            }
            VoucherOrder voucherOrder = new VoucherOrder();
            voucherOrder.setId(orderMessage.getOrderId());
            voucherOrder.setUserId(orderMessage.getUserId());
            voucherOrder.setVoucherId(orderMessage.getVoucherId());

            voucherOrderService.processVoucherOrder(voucherOrder);
            Long outboxId = extractOutboxId(orderMessage, message);
            if (outboxId != null) {
                outboxService.removeById(outboxId);
            }
            seckillMetrics.markStreamConsume();
            channel.basicAck(deliveryTag, false);
        } catch (Exception e) {
            seckillMetrics.markStreamError();
            log.error("consume seckill order message failed, payload={}", payload, e);
            channel.basicNack(deliveryTag, false, false);
        }
    }

    private Long extractOutboxId(SeckillOrderMessage orderMessage, Message message) {
        if (orderMessage.getOutboxId() != null) {
            return orderMessage.getOutboxId();
        }
        Object header = message.getMessageProperties().getHeaders().get(SeckillMqConstants.HEADER_OUTBOX_ID);
        if (header instanceof Number) {
            return ((Number) header).longValue();
        }
        if (header instanceof String) {
            try {
                return Long.valueOf((String) header);
            } catch (NumberFormatException e) {
                log.warn("invalid outbox id header in consumer: {}", header);
            }
        }
        return null;
    }
}
