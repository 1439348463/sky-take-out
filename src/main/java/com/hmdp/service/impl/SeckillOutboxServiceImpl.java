package com.hmdp.service.impl;

import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.SeckillOrderMessage;
import com.hmdp.entity.VoucherOrderOutbox;
import com.hmdp.mapper.VoucherOrderOutboxMapper;
import com.hmdp.service.ISeckillOutboxService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;

@Slf4j
@Service
public class SeckillOutboxServiceImpl extends ServiceImpl<VoucherOrderOutboxMapper, VoucherOrderOutbox>
        implements ISeckillOutboxService {

    private static final int MAX_PUBLISH_RETRY = 10;

    @Override
    public VoucherOrderOutbox createOrderOutbox(Long orderId, Long userId, Long voucherId) {
        long outboxId = orderId;
        SeckillOrderMessage message = new SeckillOrderMessage();
        message.setOutboxId(outboxId);
        message.setOrderId(orderId);
        message.setUserId(userId);
        message.setVoucherId(voucherId);

        VoucherOrderOutbox outbox = new VoucherOrderOutbox();
        outbox.setId(outboxId);
        outbox.setOrderId(orderId);
        outbox.setUserId(userId);
        outbox.setVoucherId(voucherId);
        outbox.setPayload(JSONUtil.toJsonStr(message));
        outbox.setStatus(STATUS_NEW);
        outbox.setRetryCount(0);
        outbox.setNextRetryTime(LocalDateTime.now());
        outbox.setLastError("");
        save(outbox);
        return outbox;
    }

    @Override
    public List<VoucherOrderOutbox> findPublishCandidates(int limit) {
        List<VoucherOrderOutbox> list = query()
                .in("status", STATUS_NEW, STATUS_PUBLISHING)
                .le("next_retry_time", LocalDateTime.now())
                .orderByAsc("next_retry_time")
                .last("limit " + limit)
                .list();
        return list == null ? Collections.emptyList() : list;
    }

    @Override
    public boolean markPublishing(Long outboxId, int expectedRetryCount) {
        return update()
                .set("status", STATUS_PUBLISHING)
                .set("updated_at", LocalDateTime.now())
                .eq("id", outboxId)
                .eq("retry_count", expectedRetryCount)
                .in("status", STATUS_NEW, STATUS_PUBLISHING)
                .update();
    }

    @Override
    public void markConfirmed(Long outboxId) {
        update()
                .set("status", STATUS_CONFIRMED)
                .set("updated_at", LocalDateTime.now())
                .eq("id", outboxId)
                .update();
    }

    @Override
    public void markPublishFailed(Long outboxId, String reason) {
        VoucherOrderOutbox outbox = getById(outboxId);
        if (outbox == null) {
            return;
        }
        int currentRetry = outbox.getRetryCount() == null ? 0 : outbox.getRetryCount();
        int nextRetry = Math.min(currentRetry + 1, MAX_PUBLISH_RETRY);
        int backoffSeconds = (int) Math.min(60L, 1L << Math.min(nextRetry, 6));
        int nextStatus = nextRetry >= MAX_PUBLISH_RETRY ? STATUS_FAILED : STATUS_NEW;
        update()
                .set("status", nextStatus)
                .set("retry_count", nextRetry)
                .set("last_error", reason == null ? "" : reason.substring(0, Math.min(reason.length(), 250)))
                .set("next_retry_time", LocalDateTime.now().plusSeconds(backoffSeconds))
                .set("updated_at", LocalDateTime.now())
                .eq("id", outboxId)
                .update();
    }

    @Override
    public void markCompensatedFailed(Long outboxId, String reason) {
        update()
                .set("status", STATUS_FAILED)
                .set("last_error", reason == null ? "" : reason.substring(0, Math.min(reason.length(), 250)))
                .set("updated_at", LocalDateTime.now())
                .eq("id", outboxId)
                .update();
    }
}
