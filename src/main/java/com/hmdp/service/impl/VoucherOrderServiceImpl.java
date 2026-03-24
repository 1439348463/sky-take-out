package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.metrics.SeckillMetrics;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.core.io.ClassPathResource;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    private static final String STREAM_ORDERS_KEY = "stream.orders";
    private static final String STREAM_GROUP_NAME = "g1";
    private final String consumerName = "c-" + UUID.randomUUID().toString().substring(0, 8);

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private RedisIdWorker redisIdWorker;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private SeckillMetrics seckillMetrics;

    @Resource
    private IVoucherOrderService transactionalOrderService;

    private final ExecutorService seckillOrderExecutor = Executors.newSingleThreadExecutor();
    private volatile boolean running = true;

    @PostConstruct
    private void init() {
        createStreamGroupIfNotExists();
        log.info("seckill stream consumer started, group={}, consumer={}", STREAM_GROUP_NAME, consumerName);
        seckillOrderExecutor.submit(new VoucherOrderHandler());
    }

    @PreDestroy
    private void destroy() {
        running = false;
        seckillOrderExecutor.shutdownNow();
        log.info("seckill stream consumer stopping, group={}, consumer={}", STREAM_GROUP_NAME, consumerName);
    }

    private class VoucherOrderHandler implements Runnable {
        @Override
        public void run() {
            while (running && !Thread.currentThread().isInterrupted()) {
                try {
                    // 1.获取消息队列中的订单信息
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from(STREAM_GROUP_NAME, consumerName),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(STREAM_ORDERS_KEY, ReadOffset.lastConsumed())
                    );
                    // 2.判断消息获取是否成功
                    if(list == null || list.isEmpty()) {
                        // 2.1如果获取失败，说明没有消息，继续下一次循环
                        continue;
                    }
                    // 3.解析消息中的订单信息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    // 3.如果获取成功，可以下单
                    seckillMetrics.markStreamConsume();
                    handleVoucherOrder(voucherOrder);
                    // 4.ACK确认
                    stringRedisTemplate.opsForStream().acknowledge(STREAM_ORDERS_KEY, STREAM_GROUP_NAME, record.getId());
                } catch (Exception e) {
                    if (!running || Thread.currentThread().isInterrupted()) {
                        log.debug("seckill stream consumer interrupted while shutting down, consumer={}", consumerName);
                        return;
                    }
                    log.error("处理订单异常", e);
                    seckillMetrics.markStreamError();
                    handlePendingList();
                }
            }
        }

        private void handlePendingList() {

            while (running && !Thread.currentThread().isInterrupted()) {
                try {
                    // 1.获取消息队列中的订单信息
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from(STREAM_GROUP_NAME, consumerName),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create(STREAM_ORDERS_KEY, ReadOffset.from("0"))
                    );
                    // 2.判断消息获取是否成功
                    if(list == null || list.isEmpty()) {
                        // 2.1如果获取失败，说明没有消息，继续下一次循环
                        break;
                    }
                    // 3.解析消息中的订单信息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    // 3.如果获取成功，可以下单
                    seckillMetrics.markPendingConsume();
                    handleVoucherOrder(voucherOrder);
                    // 4.ACK确认
                    stringRedisTemplate.opsForStream().acknowledge(STREAM_ORDERS_KEY, STREAM_GROUP_NAME, record.getId());
                } catch (Exception e) {
                    if (!running || Thread.currentThread().isInterrupted()) {
                        log.debug("pending-list consumer interrupted while shutting down, consumer={}", consumerName);
                        return;
                    }
                    log.error("处理pending-list订单异常", e);
                    seckillMetrics.markStreamError();
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }
        }
    }

    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        //创建锁对象
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        //获取锁
        boolean isLock = lock.tryLock();
        //判断是否获取锁成功
        if (!isLock) {
            log.warn("不允许重复下单，userId={}, voucherId={}, orderId={}",
                    userId, voucherOrder.getVoucherId(), voucherOrder.getId());
            seckillMetrics.markLockFailed();
            return;
        }
        try {
            transactionalOrderService.createVoucherOrder(voucherOrder);
        } finally {
            lock.unlock();
        }
    }

    @Resource
    private RedissonClient redissonClient;

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    @Override
    public Result seckillVoucher(Long voucherId) {
        seckillMetrics.markRequest();
        Long userId = UserHolder.getUser().getId();
        long orderId = redisIdWorker.nextId("order");
        //1.执行lua脚本
        long luaStartNanos = System.nanoTime();
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString(), String.valueOf(orderId)
        );
        seckillMetrics.recordLuaExecute(System.nanoTime() - luaStartNanos);
        if (result == null) {
            seckillMetrics.markCreateFailed();
            log.error("执行秒杀脚本失败，返回值为空，voucherId={}, userId={}, orderId={}", voucherId, userId, orderId);
            return Result.fail("下单失败，请稍后重试");
        }
        //2.判断结果是否为0
        int r = result.intValue();
        //2.1不为0，代表没有购买资格
        if (r != 0) {
            if (r == 1) {
                seckillMetrics.markRejectStock();
            } else {
                seckillMetrics.markRejectDuplicate();
            }
            log.info("秒杀请求被拒绝，voucherId={}, userId={}, orderId={}, reason={}",
                    voucherId, userId, orderId, r == 1 ? "stock_not_enough" : "duplicate_order");
            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
        }
        seckillMetrics.markPass();
        log.info("秒杀请求通过，voucherId={}, userId={}, orderId={}", voucherId, userId, orderId);
        //4.返回订单id
        return Result.ok(orderId);
    }

    /*@Override
    //@Transactional
    public Result seckillVoucher(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        //1.执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString()
        );
        //2.判断结果是否为0
        int r = result.intValue();
        //2.1不为0，代表没有购买资格
        if (r != 0) {
            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
        }
        //2.2为0，有购买资格，把下单信息保存到阻塞队列
        VoucherOrder voucherOrder = new VoucherOrder();
        long orderId = redisIdWorker.nextId("order");
        voucherOrder.setId(orderId);
        voucherOrder.setUserId(userId);
        voucherOrder.setVoucherId(voucherId);

        orderTasks.add(voucherOrder);
        //3.获取代理对象
        proxy = (IVoucherOrderService) AopContext.currentProxy();

        //4.返回订单id
        return Result.ok(orderId);
    }*/

    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        long createStartNanos = System.nanoTime();
        //6.一人一单
        Long userId = voucherOrder.getUserId();

        int count = query().eq("user_id", userId).eq("voucher_id", voucherOrder.getVoucherId()).count();
        if (count > 0) {
            seckillMetrics.markCreateDuplicate();
            log.warn("用户重复下单，userId={}, voucherId={}, orderId={}",
                    userId, voucherOrder.getVoucherId(), voucherOrder.getId());
            seckillMetrics.recordCreateOrder(System.nanoTime() - createStartNanos);
            return ;
        }

        //5.扣减库存
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherOrder.getVoucherId())
                //.eq("stock", voucher.getStock())
                .gt("stock", 0)
                .update();
        if (!success) {
            seckillMetrics.markCreateStockFailed();
            log.warn("创建订单扣减库存失败，userId={}, voucherId={}, orderId={}",
                    userId, voucherOrder.getVoucherId(), voucherOrder.getId());
            seckillMetrics.recordCreateOrder(System.nanoTime() - createStartNanos);
            return ;
        }

        try {
            boolean saved = save(voucherOrder);
            if (!saved) {
                seckillMetrics.markCreateFailed();
                log.error("保存订单失败，userId={}, voucherId={}, orderId={}",
                        userId, voucherOrder.getVoucherId(), voucherOrder.getId());
                return;
            }
            seckillMetrics.markCreateSuccess();
            log.info("创建订单成功，userId={}, voucherId={}, orderId={}",
                    userId, voucherOrder.getVoucherId(), voucherOrder.getId());
        } catch (DuplicateKeyException e) {
            seckillMetrics.markCreateDuplicate();
            log.warn("命中数据库唯一索引，重复下单被拦截，userId={}, voucherId={}, orderId={}",
                    userId, voucherOrder.getVoucherId(), voucherOrder.getId());
        } finally {
            seckillMetrics.recordCreateOrder(System.nanoTime() - createStartNanos);
        }
    }

    private void createStreamGroupIfNotExists() {
        try {
            stringRedisTemplate.opsForStream().createGroup(STREAM_ORDERS_KEY, ReadOffset.latest(), STREAM_GROUP_NAME);
            log.info("创建stream消费组成功，stream={}, group={}", STREAM_ORDERS_KEY, STREAM_GROUP_NAME);
        } catch (Exception e) {
            String message = e.getMessage();
            if (message != null && message.contains("BUSYGROUP")) {
                return;
            }
            try {
                stringRedisTemplate.opsForStream().add(STREAM_ORDERS_KEY, Collections.singletonMap("init", "0"));
                stringRedisTemplate.opsForStream().createGroup(STREAM_ORDERS_KEY, ReadOffset.latest(), STREAM_GROUP_NAME);
                log.info("stream不存在，已自动创建并创建消费组，stream={}, group={}", STREAM_ORDERS_KEY, STREAM_GROUP_NAME);
            } catch (Exception ex) {
                String nestedMessage = ex.getMessage();
                if (nestedMessage == null || !nestedMessage.contains("BUSYGROUP")) {
                    throw ex;
                }
            }
        }
    }

    /*@Transactional
    public Result createVoucherOrder(Long voucherId) {

        //6.一人一单
        Long userId = UserHolder.getUser().getId();


        int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
        if (count > 0) {
            return Result.fail("用户已经购买过一次");
        }

        //5.扣减库存
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherId)
                //.eq("stock", voucher.getStock())
                .gt("stock", 0)
                .update();
        if (!success) {
            return Result.fail("库存不足");
        }

        //6.创建订单
        VoucherOrder voucherOrder = new VoucherOrder();
        long orderId = redisIdWorker.nextId("order");
        voucherOrder.setId(orderId);
        voucherOrder.setUserId(userId);
        voucherOrder.setVoucherId(voucherId);
        save(voucherOrder);

        //7.返回订单id
        return Result.ok(orderId);
    }*/

    /*@Override
    //@Transactional
    public Result seckillVoucher(Long voucherId) {
        //1.查询优惠券
        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
        //2.判断秒杀是否开始
        if (voucher.getBeginTime().isAfter(LocalDateTime.now())) {
            return Result.fail("秒杀尚未开始");
        }
        //3.判断秒杀是否已经结束
        if (voucher.getEndTime().isBefore(LocalDateTime.now())) {
            return Result.fail("秒杀已经结束");
        }
        //4.判断库存是否充足
        if (voucher.getStock() < 1) {
            return Result.fail("库存不足");
        }

        Long userId = UserHolder.getUser().getId();

        //创建锁对象
        //SimpleRedisLock lock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        //获取锁
        boolean isLock = lock.tryLock();
        //判断是否获取锁成功
        if (!isLock) {
            return Result.fail("不允许重复下单");
        }
        //开始听不懂
        //获取代理对象（事务 ）
        try {
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            return proxy.createVoucherOrder(voucherId);
        } finally {
            lock.unlock();
        }
    }*/
}
