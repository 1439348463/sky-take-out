package com.hmdp.utils;

public final class SeckillMqConstants {

    private SeckillMqConstants() {
    }

    public static final String EXCHANGE_ORDER = "seckill.order.ex";
    public static final String ROUTING_KEY_ORDER_CREATED = "seckill.order.created";
    public static final String QUEUE_ORDER = "seckill.order.q";

    public static final String EXCHANGE_ORDER_DLX = "seckill.order.dlx";
    public static final String ROUTING_KEY_ORDER_DLX = "seckill.order.dlq";
    public static final String QUEUE_ORDER_DLQ = "seckill.order.dlq";

    public static final String HEADER_OUTBOX_ID = "x-outbox-id";
    public static final String HEADER_PUBLISH_RETRY_COUNT = "x-publish-retry-count";
}
