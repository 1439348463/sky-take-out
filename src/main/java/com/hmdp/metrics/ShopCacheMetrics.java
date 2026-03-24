package com.hmdp.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

@Component
public class ShopCacheMetrics {

    private final Counter queryTotalCounter;
    private final Counter cacheHitCounter;
    private final Counter cacheStaleCounter;
    private final Counter cacheMissCounter;
    private final Counter dbFallbackCounter;
    private final Counter rebuildStartCounter;
    private final Counter rebuildSuccessCounter;
    private final Counter rebuildFailCounter;
    private final Timer queryTimer;

    public ShopCacheMetrics(MeterRegistry meterRegistry) {
        this.queryTotalCounter = meterRegistry.counter("hmdp.shop.query.total");
        this.cacheHitCounter = meterRegistry.counter("hmdp.shop.query.cache.hit");
        this.cacheStaleCounter = meterRegistry.counter("hmdp.shop.query.cache.stale");
        this.cacheMissCounter = meterRegistry.counter("hmdp.shop.query.cache.miss");
        this.dbFallbackCounter = meterRegistry.counter("hmdp.shop.query.db.fallback");
        this.rebuildStartCounter = meterRegistry.counter("hmdp.shop.query.rebuild.start");
        this.rebuildSuccessCounter = meterRegistry.counter("hmdp.shop.query.rebuild.success");
        this.rebuildFailCounter = meterRegistry.counter("hmdp.shop.query.rebuild.fail");
        this.queryTimer = meterRegistry.timer("hmdp.shop.query");
    }

    public void markQueryTotal() {
        queryTotalCounter.increment();
    }

    public void markCacheHit() {
        cacheHitCounter.increment();
    }

    public void markCacheStale() {
        cacheStaleCounter.increment();
    }

    public void markCacheMiss() {
        cacheMissCounter.increment();
    }

    public void markDbFallback() {
        dbFallbackCounter.increment();
    }

    public void markRebuildStart() {
        rebuildStartCounter.increment();
    }

    public void markRebuildSuccess() {
        rebuildSuccessCounter.increment();
    }

    public void markRebuildFail() {
        rebuildFailCounter.increment();
    }

    public void recordQuery(long nanos) {
        queryTimer.record(nanos, TimeUnit.NANOSECONDS);
    }
}
