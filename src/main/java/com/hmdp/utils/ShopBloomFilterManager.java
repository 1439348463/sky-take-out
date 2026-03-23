package com.hmdp.utils;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RBloomFilter;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.stereotype.Component;
import org.springframework.scheduling.annotation.Scheduled;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
public class ShopBloomFilterManager {

    private static final String SHOP_BLOOM_REBUILD_LOCK_KEY = "lock:shop:bloom:rebuild";

    @Resource
    private RedissonClient redissonClient;

    @Resource
    private ShopMapper shopMapper;

    private volatile RBloomFilter<Long> bloomFilter;

    @PostConstruct
    public void init() {
        rebuild();
    }

    @Scheduled(cron = "0 30 3 * * ?")
    public void scheduledRebuild() {
        rebuild();
    }

    public boolean mightContain(Long shopId) {
        if (shopId == null || shopId <= 0) {
            return false;
        }
        RBloomFilter<Long> filter = bloomFilter;
        if (filter == null || !filter.isExists()) {
            // 过滤器异常时降级放行，避免误拦截真实数据
            return true;
        }
        return filter.contains(shopId);
    }

    public void add(Long shopId) {
        if (shopId == null || shopId <= 0) {
            return;
        }
        RBloomFilter<Long> filter = bloomFilter;
        if (filter == null) {
            filter = redissonClient.getBloomFilter(RedisConstants.SHOP_ID_BLOOM_KEY);
            bloomFilter = filter;
        }
        if (!filter.isExists()) {
            filter.tryInit(
                    RedisConstants.SHOP_ID_BLOOM_EXPECTED_INSERTIONS,
                    RedisConstants.SHOP_ID_BLOOM_FPP
            );
        }
        filter.add(shopId);
    }

    public void rebuild() {
        RLock lock = redissonClient.getLock(SHOP_BLOOM_REBUILD_LOCK_KEY);
        boolean isLocked = false;
        try {
            isLocked = lock.tryLock(1, 30, TimeUnit.MINUTES);
            if (!isLocked) {
                log.debug("skip shop bloom rebuild, lock not acquired");
                return;
            }
            doRebuild();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("shop bloom rebuild interrupted", e);
        } finally {
            if (isLocked && lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }

    private void doRebuild() {
        String tmpKey = RedisConstants.SHOP_ID_BLOOM_KEY + ":tmp";
        RBloomFilter<Long> tmpFilter = redissonClient.getBloomFilter(tmpKey);
        tmpFilter.delete();
        tmpFilter.tryInit(calculateExpectedInsertions(), RedisConstants.SHOP_ID_BLOOM_FPP);

        List<Shop> shops = shopMapper.selectList(new QueryWrapper<Shop>().select("id"));
        for (Shop shop : shops) {
            if (shop.getId() != null) {
                tmpFilter.add(shop.getId());
            }
        }

        tmpFilter.rename(RedisConstants.SHOP_ID_BLOOM_KEY);
        bloomFilter = redissonClient.getBloomFilter(RedisConstants.SHOP_ID_BLOOM_KEY);
        log.info("shop bloom filter rebuild complete, dbSize={}", shops.size());
    }

    private long calculateExpectedInsertions() {
        Integer total = shopMapper.selectCount(null);
        return Math.max(
                (long) (total == null ? 0 : total) * 2,
                RedisConstants.SHOP_ID_BLOOM_EXPECTED_INSERTIONS
        );
    }
}
