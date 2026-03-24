package com.hmdp.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

@Component
public class SeckillMetrics {

    private final Counter requestTotalCounter;
    private final Counter passCounter;
    private final Counter rejectStockCounter;
    private final Counter rejectDuplicateCounter;
    private final Counter streamConsumeCounter;
    private final Counter streamErrorCounter;
    private final Counter streamPendingCounter;
    private final Counter lockFailedCounter;
    private final Counter createSuccessCounter;
    private final Counter createDuplicateCounter;
    private final Counter createStockFailedCounter;
    private final Counter createFailedCounter;
    private final Timer luaExecuteTimer;
    private final Timer createOrderTimer;

    public SeckillMetrics(MeterRegistry meterRegistry) {
        this.requestTotalCounter = meterRegistry.counter("hmdp.seckill.request.total");
        this.passCounter = meterRegistry.counter("hmdp.seckill.request.pass");
        this.rejectStockCounter = meterRegistry.counter("hmdp.seckill.request.reject.stock");
        this.rejectDuplicateCounter = meterRegistry.counter("hmdp.seckill.request.reject.duplicate");
        this.streamConsumeCounter = meterRegistry.counter("hmdp.seckill.stream.consume");
        this.streamErrorCounter = meterRegistry.counter("hmdp.seckill.stream.error");
        this.streamPendingCounter = meterRegistry.counter("hmdp.seckill.stream.pending.consume");
        this.lockFailedCounter = meterRegistry.counter("hmdp.seckill.order.lock.fail");
        this.createSuccessCounter = meterRegistry.counter("hmdp.seckill.order.create.success");
        this.createDuplicateCounter = meterRegistry.counter("hmdp.seckill.order.create.duplicate");
        this.createStockFailedCounter = meterRegistry.counter("hmdp.seckill.order.create.stock.fail");
        this.createFailedCounter = meterRegistry.counter("hmdp.seckill.order.create.fail");
        this.luaExecuteTimer = meterRegistry.timer("hmdp.seckill.lua.execute");
        this.createOrderTimer = meterRegistry.timer("hmdp.seckill.order.create");
    }

    public void markRequest() {
        requestTotalCounter.increment();
    }

    public void markPass() {
        passCounter.increment();
    }

    public void markRejectStock() {
        rejectStockCounter.increment();
    }

    public void markRejectDuplicate() {
        rejectDuplicateCounter.increment();
    }

    public void markStreamConsume() {
        streamConsumeCounter.increment();
    }

    public void markStreamError() {
        streamErrorCounter.increment();
    }

    public void markPendingConsume() {
        streamPendingCounter.increment();
    }

    public void markLockFailed() {
        lockFailedCounter.increment();
    }

    public void markCreateSuccess() {
        createSuccessCounter.increment();
    }

    public void markCreateDuplicate() {
        createDuplicateCounter.increment();
    }

    public void markCreateStockFailed() {
        createStockFailedCounter.increment();
    }

    public void markCreateFailed() {
        createFailedCounter.increment();
    }

    public void recordLuaExecute(long nanos) {
        luaExecuteTimer.record(nanos, TimeUnit.NANOSECONDS);
    }

    public void recordCreateOrder(long nanos) {
        createOrderTimer.record(nanos, TimeUnit.NANOSECONDS);
    }
}
