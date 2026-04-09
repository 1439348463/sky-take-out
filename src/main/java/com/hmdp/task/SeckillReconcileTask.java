package com.hmdp.task;

import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.entity.VoucherOrderOutbox;
import com.hmdp.service.ISeckillOutboxService;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.impl.VoucherOrderServiceImpl;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.SECKILL_ORDER_KEY;

@Slf4j
@Component
public class SeckillReconcileTask {

    private static final String DEAD_OUTBOX_LOCK = "lock:seckill:reconcile:dead-outbox";
    private static final String ORPHAN_REDIS_LOCK = "lock:seckill:reconcile:orphan-redis";

    @Resource
    private ISeckillOutboxService outboxService;

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private VoucherOrderServiceImpl voucherOrderService;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private RedissonClient redissonClient;

    @Value("${hmdp.seckill.reconcile.dead-delay-ms:60000}")
    private long deadDelayMs;

    @Value("${hmdp.seckill.reconcile.dead-batch-size:100}")
    private int deadBatchSize;

    @Value("${hmdp.seckill.reconcile.orphan-delay-ms:300000}")
    private long orphanDelayMs;

    @Value("${hmdp.seckill.reconcile.orphan-scan-count:200}")
    private long orphanScanCount;

    @Scheduled(fixedDelayString = "${hmdp.seckill.reconcile.dead-delay-ms:60000}")
    public void reconcileDeadOutboxes() {
        RLock lock = redissonClient.getLock(DEAD_OUTBOX_LOCK);
        boolean locked = lock.tryLock();
        if (!locked) {
            return;
        }
        int scanned = 0;
        int removed = 0;
        int compensated = 0;
        int skipped = 0;
        try {
            List<VoucherOrderOutbox> deadOutboxes = outboxService.query()
                    .eq("status", ISeckillOutboxService.STATUS_DEAD)
                    .orderByAsc("updated_at")
                    .last("limit " + deadBatchSize)
                    .list();
            for (VoucherOrderOutbox outbox : deadOutboxes) {
                scanned++;
                if (outbox == null || outbox.getId() == null) {
                    skipped++;
                    continue;
                }
                VoucherOrder existingOrder = outbox.getOrderId() == null ? null : voucherOrderService.getById(outbox.getOrderId());
                if (existingOrder != null) {
                    if (outboxService.removeById(outbox.getId())) {
                        removed++;
                    } else {
                        skipped++;
                    }
                    continue;
                }
                if (outbox.getVoucherId() == null || outbox.getUserId() == null) {
                    outboxService.markDead(outbox.getId(), "dead_reconcile_missing_fields");
                    skipped++;
                    continue;
                }
                boolean compensatedReservation = voucherOrderService.compensateReservation(outbox.getVoucherId(), outbox.getUserId());
                if (!compensatedReservation) {
                    outboxService.markDead(outbox.getId(), "dead_reconcile_compensate_noop");
                    skipped++;
                    continue;
                }
                compensated++;
                if (outboxService.removeById(outbox.getId())) {
                    removed++;
                }
            }
        } finally {
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
        if (scanned > 0 || removed > 0 || compensated > 0) {
            log.info("dead outbox reconcile finished, scanned={}, removed={}, compensated={}, skipped={}, delayMs={}, batchSize={}",
                    scanned, removed, compensated, skipped, deadDelayMs, deadBatchSize);
        }
    }

    @Scheduled(fixedDelayString = "${hmdp.seckill.reconcile.orphan-delay-ms:300000}")
    public void reconcileOrphanRedisReservations() {
        RLock lock = redissonClient.getLock(ORPHAN_REDIS_LOCK);
        boolean locked = lock.tryLock();
        if (!locked) {
            return;
        }
        int scannedVouchers = 0;
        int scannedUsers = 0;
        int compensated = 0;
        int skipped = 0;
        try {
            List<SeckillVoucher> vouchers = seckillVoucherService.list();
            for (SeckillVoucher voucher : vouchers) {
                if (voucher == null || voucher.getVoucherId() == null) {
                    continue;
                }
                Long voucherId = voucher.getVoucherId();
                String orderKey = SECKILL_ORDER_KEY + voucherId;
                Boolean hasKey = stringRedisTemplate.hasKey(orderKey);
                if (!Boolean.TRUE.equals(hasKey)) {
                    continue;
                }
                scannedVouchers++;
                Set<String> orderedUsers = voucherOrderService.query()
                        .select("user_id")
                        .eq("voucher_id", voucherId)
                        .list()
                        .stream()
                        .map(VoucherOrder::getUserId)
                        .filter(userId -> userId != null)
                        .map(String::valueOf)
                        .collect(Collectors.toSet());
                Set<String> outboxUsers = outboxService.query()
                        .select("user_id")
                        .eq("voucher_id", voucherId)
                        .list()
                        .stream()
                        .map(VoucherOrderOutbox::getUserId)
                        .filter(userId -> userId != null)
                        .map(String::valueOf)
                        .collect(Collectors.toSet());
                try (Cursor<String> cursor = stringRedisTemplate.opsForSet().scan(
                        orderKey,
                        ScanOptions.scanOptions()
                                .count(orphanScanCount)
                                .build())) {
                    while (cursor.hasNext()) {
                        String userIdValue = cursor.next();
                        scannedUsers++;
                        if (orderedUsers.contains(userIdValue) || outboxUsers.contains(userIdValue)) {
                            skipped++;
                            continue;
                        }
                        Long userId = parseUserId(userIdValue);
                        if (userId == null) {
                            skipped++;
                            continue;
                        }
                        boolean compensatedReservation = voucherOrderService.compensateReservation(voucherId, userId);
                        if (compensatedReservation) {
                            compensated++;
                        } else {
                            skipped++;
                        }
                    }
                } catch (Exception e) {
                    log.error("scan orphan redis reservations failed, voucherId={}", voucherId, e);
                }
            }
        } finally {
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
        if (scannedUsers > 0 || compensated > 0) {
            log.info("orphan redis reconcile finished, vouchers={}, users={}, compensated={}, skipped={}, delayMs={}, scanCount={}",
                    scannedVouchers, scannedUsers, compensated, skipped, orphanDelayMs, orphanScanCount);
        }
    }

    private Long parseUserId(String userIdValue) {
        if (userIdValue == null || userIdValue.isEmpty()) {
            return null;
        }
        try {
            return Long.valueOf(userIdValue);
        } catch (NumberFormatException e) {
            log.warn("skip invalid seckill user id in redis set, userId={}", userIdValue);
            return null;
        }
    }
}
