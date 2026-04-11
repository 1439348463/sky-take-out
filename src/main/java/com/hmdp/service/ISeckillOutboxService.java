package com.hmdp.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.hmdp.entity.VoucherOrderOutbox;

import java.util.List;

public interface ISeckillOutboxService extends IService<VoucherOrderOutbox> {
    int STATUS_NEW = 0;
    int STATUS_SENDING = 1;
    int STATUS_DELIVERED = 2;
    int STATUS_DEAD = 5;

    VoucherOrderOutbox createOrderOutbox(Long orderId, Long userId, Long voucherId);

    List<VoucherOrderOutbox> findPublishCandidates(int limit);

    boolean markSending(Long outboxId, int expectedRetryCount);

    void markDelivered(Long outboxId, int expectedRetryCount);

    void markPublishFailed(Long outboxId, String reason, Integer expectedRetryCount);

    default void markPublishFailed(Long outboxId, String reason) {
        markPublishFailed(outboxId, reason, null);
    }

    void markReturned(Long outboxId, String reason, Integer expectedRetryCount);

    boolean recordDlqReplay(Long outboxId, int expectedReplayCount);

    void markDead(Long outboxId, String reason);
}
