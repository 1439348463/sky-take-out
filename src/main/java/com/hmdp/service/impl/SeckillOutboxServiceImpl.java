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
    private static final int PUBLISHING_TIMEOUT_SECONDS = 30;

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
        boolean saved = save(outbox);
        if (!saved) {
            throw new IllegalStateException("save voucher order outbox failed, outboxId=" + outboxId);
        }
        return outbox;
    }

    @Override
    public List<VoucherOrderOutbox> findPublishCandidates(int limit) {
        List<VoucherOrderOutbox> list = query()
                .in("status", STATUS_NEW, STATUS_SENDING)
                .le("next_retry_time", LocalDateTime.now())
                .orderByAsc("next_retry_time")
                .last("limit " + limit)
                .list();
        return list == null ? Collections.emptyList() : list;
    }

    @Override
    public boolean markSending(Long outboxId, int expectedRetryCount) {
        LocalDateTime now = LocalDateTime.now();
        return update()
                .set("status", STATUS_SENDING)
                .set("next_retry_time", now.plusSeconds(PUBLISHING_TIMEOUT_SECONDS))
                .set("updated_at", now)
                .eq("id", outboxId)
                .eq("retry_count", expectedRetryCount)
                .in("status", STATUS_NEW, STATUS_SENDING)
                .update();
    }

    @Override
    public void markDelivered(Long outboxId, int expectedRetryCount) {
        LocalDateTime now = LocalDateTime.now();
        boolean updated = update()
                .set("status", STATUS_DELIVERED)
                .set("retry_count", 0)
                .set("last_error", "")
                .set("next_retry_time", now)
                .set("updated_at", now)
                .eq("id", outboxId)
                .eq("status", STATUS_SENDING)
                .eq("retry_count", expectedRetryCount)
                .update();
        if (!updated) {
            log.debug("skip mark delivered, outbox state already changed, outboxId={}, retry={}", outboxId, expectedRetryCount);
        }
    }

    @Override
    public void markPublishFailed(Long outboxId, String reason, Integer expectedRetryCount) {
        Integer currentRetry = expectedRetryCount;
        if (currentRetry == null) {
            VoucherOrderOutbox outbox = getById(outboxId);
            if (outbox == null) {
                return;
            }
            currentRetry = outbox.getRetryCount() == null ? 0 : outbox.getRetryCount();
        }
        LocalDateTime now = LocalDateTime.now();
        int nextRetry = Math.min(currentRetry + 1, MAX_PUBLISH_RETRY);
        int backoffSeconds = (int) Math.min(60L, 1L << Math.min(nextRetry, 6));
        int nextStatus = nextRetry >= MAX_PUBLISH_RETRY ? STATUS_DEAD : STATUS_NEW;
        boolean updated = update()
                .set("status", nextStatus)
                .set("retry_count", nextRetry)
                .set("last_error", reason == null ? "" : reason.substring(0, Math.min(reason.length(), 250)))
                .set("next_retry_time", now.plusSeconds(backoffSeconds))
                .set("updated_at", now)
                .eq("id", outboxId)
                .eq("retry_count", currentRetry)
                .eq("status", STATUS_SENDING)
                .update();
        if (!updated) {
            log.debug("skip mark publish failed, outbox state already changed, outboxId={}, retry={}", outboxId, currentRetry);
        }
    }

    @Override
    public void markReturned(Long outboxId, String reason, Integer expectedRetryCount) {
        Integer currentRetry = expectedRetryCount;
        if (currentRetry == null) {
            VoucherOrderOutbox outbox = getById(outboxId);
            if (outbox == null) {
                return;
            }
            currentRetry = outbox.getRetryCount() == null ? 0 : outbox.getRetryCount();
        }
        LocalDateTime now = LocalDateTime.now();
        int nextRetry = Math.min(currentRetry + 1, MAX_PUBLISH_RETRY);
        int backoffSeconds = (int) Math.min(60L, 1L << Math.min(nextRetry, 6));
        int nextStatus = nextRetry >= MAX_PUBLISH_RETRY ? STATUS_DEAD : STATUS_NEW;

        boolean updatedSending = update()
                .set("status", nextStatus)
                .set("retry_count", nextRetry)
                .set("last_error", reason == null ? "" : reason.substring(0, Math.min(reason.length(), 250)))
                .set("next_retry_time", now.plusSeconds(backoffSeconds))
                .set("updated_at", now)
                .eq("id", outboxId)
                .eq("status", STATUS_SENDING)
                .eq("retry_count", currentRetry)
                .update();
        if (updatedSending) {
            return;
        }

        VoucherOrderOutbox outbox = getById(outboxId);
        if (outbox == null || outbox.getStatus() == null || outbox.getStatus() != STATUS_DELIVERED) {
            return;
        }
        int deliveredRetry = outbox.getRetryCount() == null ? 0 : outbox.getRetryCount();
        int deliveredNextRetry = Math.min(deliveredRetry + 1, MAX_PUBLISH_RETRY);
        int deliveredNextStatus = deliveredNextRetry >= MAX_PUBLISH_RETRY ? STATUS_DEAD : STATUS_NEW;
        update()
                .set("status", deliveredNextStatus)
                .set("retry_count", deliveredNextRetry)
                .set("last_error", reason == null ? "" : reason.substring(0, Math.min(reason.length(), 250)))
                .set("next_retry_time", now.plusSeconds(backoffSeconds))
                .set("updated_at", now)
                .eq("id", outboxId)
                .eq("status", STATUS_DELIVERED)
                .eq("retry_count", deliveredRetry)
                .update();
    }

    @Override
    public boolean recordDlqReplay(Long outboxId, int expectedReplayCount) {
        LocalDateTime now = LocalDateTime.now();
        return update()
                .set("retry_count", expectedReplayCount + 1)
                .set("last_error", "dlq_replay_" + (expectedReplayCount + 1))
                .set("updated_at", now)
                .eq("id", outboxId)
                .eq("status", STATUS_DELIVERED)
                .eq("retry_count", expectedReplayCount)
                .update();
    }

    @Override
    public void markDead(Long outboxId, String reason) {
        LocalDateTime now = LocalDateTime.now();
        update()
                .set("status", STATUS_DEAD)
                .set("last_error", reason == null ? "" : reason.substring(0, Math.min(reason.length(), 250)))
                .set("updated_at", now)
                .eq("id", outboxId)
                .update();
    }
}
