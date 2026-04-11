package com.hmdp.dto;

import org.springframework.amqp.rabbit.connection.CorrelationData;

public class SeckillCorrelationData extends CorrelationData {
    private final int retryCount;

    public SeckillCorrelationData(String id, int retryCount) {
        super(id);
        this.retryCount = retryCount;
    }

    public int getRetryCount() {
        return retryCount;
    }
}
