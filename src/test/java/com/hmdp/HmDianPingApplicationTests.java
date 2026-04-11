package com.hmdp;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.RandomUtil;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Shop;
import com.hmdp.entity.User;
import com.hmdp.service.IUserService;
import com.hmdp.service.impl.ShopServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;

import javax.annotation.Resource;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.LOGIN_USER_KEY;
import static com.hmdp.utils.RedisConstants.LOGIN_USER_TTL;
import static com.hmdp.utils.RedisConstants.SHOP_GEO_KEY;
import static com.hmdp.utils.SystemConstants.USER_NICK_NAME_PREFIX;

@Slf4j
@SpringBootTest
class HmDianPingApplicationTests {

    private static final int NEW_USER_COUNT = 500;
    private static final int TOKEN_TOTAL_COUNT = 1000;
    private static final Path TOKEN_FILE_PATH = Paths.get("src", "test", "resources", "token.csv");

    @Resource
    private ShopServiceImpl shopService;

    @Resource
    private IUserService userService;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private RedisIdWorker redisIdWorker;

    private ExecutorService es = Executors.newFixedThreadPool(500);

    @Test
    void testIdWorder() throws InterruptedException {
        CountDownLatch countDownLatch = new CountDownLatch(300);

        Runnable task = () -> {
            for (int i = 0; i < 100; i++) {
                long id = redisIdWorker.nextId("order");
                System.out.println(id);
            }
            countDownLatch.countDown();
        };
        long begin = System.currentTimeMillis();
        for (int i = 0; i < 300; i++) {
            es.submit(task);
        }
        countDownLatch.await();
        long end = System.currentTimeMillis();
        System.out.println("time = " + (end - begin));
    }

    @Test
    void testSaveShop() throws InterruptedException {
        shopService.saveShop2Redis(10L, 10L);
    }

    @Test
    void loadShopData() {
        // 1.查询店铺信息
        List<Shop> list = shopService.list();
        // 2.把店铺分组，按照typeId一致的放到一个集合
        Map<Long, List<Shop>> map = list.stream().collect(Collectors.groupingBy(shop -> shop.getTypeId()));
        // 3.分批完成写入Redis
        for (Map.Entry<Long, List<Shop>> entry : map.entrySet()) {
            // 3.1.获取同类型店铺的集合
            List<Shop> value = entry.getValue();
            // 3.2.获取类型id
            Long typeId = entry.getKey();
            String key = SHOP_GEO_KEY + typeId;
            List<RedisGeoCommands.GeoLocation<String>> locations = new ArrayList<>();
            // 3.3.写入redis GEOADD key 经度 纬度 member
            for (Shop shop : value) {
                //stringRedisTemplate.opsForGeo().add(key, new Point(shop.getX(), shop.getY()), shop.getId().toString());
                locations.add(new RedisGeoCommands.GeoLocation<>(shop.getId().toString(), new Point(shop.getX(), shop.getY())));

            }
            stringRedisTemplate.opsForGeo().add(key, locations);
        }
    }

    @Test
    void create500NewUsersForJmeter() {
        int created = createUsers(NEW_USER_COUNT);
        log.info("已批量新增测试用户 {} 个", created);
    }

    @Test
    void create1000TokensForJmeter() throws IOException {
        long currentUserCount = userService.count();
        if (currentUserCount < TOKEN_TOTAL_COUNT) {
            int need = (int) (TOKEN_TOTAL_COUNT - currentUserCount);
            int created = createUsers(need);
            log.info("用户数不足 {}，已自动补齐 {} 个测试用户", TOKEN_TOTAL_COUNT, created);
        }
        List<User> users = userService.query()
                .orderByAsc("id")
                .last("limit " + TOKEN_TOTAL_COUNT)
                .list();
        if (users.size() < TOKEN_TOTAL_COUNT) {
            throw new IllegalStateException("自动补齐后用户数仍不足 " + TOKEN_TOTAL_COUNT + "，请检查手机号唯一约束与数据状态");
        }
        Files.createDirectories(TOKEN_FILE_PATH.getParent());
        try (BufferedWriter writer = Files.newBufferedWriter(TOKEN_FILE_PATH, StandardCharsets.UTF_8)) {
            writer.write("token,userId,phone");
            writer.newLine();
            for (User user : users) {
                String token = UUID.randomUUID().toString(true);
                UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
                Map<String, Object> userMap = BeanUtil.beanToMap(userDTO, new HashMap<>(),
                        CopyOptions.create()
                                .setIgnoreNullValue(true)
                                .setFieldValueEditor((fieldName, fieldValue) -> fieldValue == null ? null : fieldValue.toString())
                );
                String tokenKey = LOGIN_USER_KEY + token;
                stringRedisTemplate.opsForHash().putAll(tokenKey, userMap);
                stringRedisTemplate.expire(tokenKey, LOGIN_USER_TTL, TimeUnit.SECONDS);
                writer.write(token + "," + user.getId() + "," + user.getPhone());
                writer.newLine();
            }
        }
        log.info("已生成 {} 个token并写入文件: {}", TOKEN_TOTAL_COUNT, TOKEN_FILE_PATH.toAbsolutePath());
    }

    private int createUsers(int targetCreateCount) {
        Set<String> existsPhones = userService.query()
                .select("phone")
                .list()
                .stream()
                .map(User::getPhone)
                .collect(Collectors.toCollection(HashSet::new));
        long seed = userService.count();
        List<User> batch = new ArrayList<>(100);
        int created = 0;
        long cursor = 0L;
        while (created < targetCreateCount) {
            String phone = buildPhone(seed + cursor++);
            if (existsPhones.contains(phone)) {
                continue;
            }
            User user = new User();
            user.setPhone(phone);
            user.setNickName(USER_NICK_NAME_PREFIX + RandomUtil.randomString(10));
            user.setPassword("");
            user.setIcon("");
            batch.add(user);
            existsPhones.add(phone);
            created++;
            if (batch.size() == 100) {
                userService.saveBatch(batch);
                batch.clear();
            }
        }
        if (!batch.isEmpty()) {
            userService.saveBatch(batch);
        }
        return created;
    }

    private String buildPhone(long seq) {
        long suffix = seq % 100_000_000L;
        return "139" + String.format("%08d", suffix);
    }
}
