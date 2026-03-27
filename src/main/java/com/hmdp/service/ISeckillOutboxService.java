package com.hmdp.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.hmdp.entity.VoucherOrderOutbox;

import java.util.List;

public interface ISeckillOutboxService extends IService<VoucherOrderOutbox> {
    int STATUS_NEW = 0;
    int STATUS_PUBLISHING = 1;
    int STATUS_CONFIRMED = 2;
    int STATUS_FAILED = 3;

    VoucherOrderOutbox createOrderOutbox(Long orderId, Long userId, Long voucherId);

    List<VoucherOrderOutbox> findPublishCandidates(int limit);

    boolean markPublishing(Long outboxId, int expectedRetryCount);

    void markConfirmed(Long outboxId);

    void markPublishFailed(Long outboxId, String reason);

    void markCompensatedFailed(Long outboxId, String reason);
}
