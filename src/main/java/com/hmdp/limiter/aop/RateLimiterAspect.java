package com.hmdp.limiter.aop;

import com.hmdp.dto.UserDTO;
import com.hmdp.limiter.annotation.RateLimiter;
import com.hmdp.limiter.exception.RateLimitException;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import java.lang.reflect.Method;
import java.util.Collections;

@Slf4j
@Aspect
@Component
public class RateLimiterAspect {

    private static final DefaultRedisScript<Long> SLIDING_WINDOW_SCRIPT;

    static {
        SLIDING_WINDOW_SCRIPT = new DefaultRedisScript<>();
        SLIDING_WINDOW_SCRIPT.setLocation(new ClassPathResource("limiter.lua"));
        SLIDING_WINDOW_SCRIPT.setResultType(Long.class);
    }

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Before("@annotation(rateLimiter)")
    public void doBefore(JoinPoint point, RateLimiter rateLimiter) {
        String key = buildRateLimitKey(point, rateLimiter);
        Long result = executeSlidingWindowScript(key, rateLimiter.window(), rateLimiter.limit());
        if (result == null) {
            throw new IllegalStateException("执行限流脚本失败");
        }
        if (result == 0L) {
            log.warn("触发滑动窗口限流，key={}, window={}, limit={}",
                    key, rateLimiter.window(), rateLimiter.limit());
            throw new RateLimitException(rateLimiter.message());
        }
    }

    private Long executeSlidingWindowScript(String key, int window, int limit) {
        long now = System.currentTimeMillis();
        return stringRedisTemplate.execute(
                SLIDING_WINDOW_SCRIPT,
                Collections.singletonList(key),
                String.valueOf(window),
                String.valueOf(limit),
                String.valueOf(now)
        );
    }

    private String buildRateLimitKey(JoinPoint point, RateLimiter rateLimiter) {
        MethodSignature signature = (MethodSignature) point.getSignature();
        Method method = signature.getMethod();

        StringBuilder keyBuilder = new StringBuilder(rateLimiter.key());
        if (keyBuilder.length() > 0 && keyBuilder.charAt(keyBuilder.length() - 1) != ':') {
            keyBuilder.append(':');
        }
        keyBuilder.append(method.getDeclaringClass().getName())
                .append(':')
                .append(method.getName());

        switch (rateLimiter.type()) {
            case IP:
                keyBuilder.append(":ip:").append(getClientIp());
                break;
            case USER:
                keyBuilder.append(":user:").append(getCurrentUserId());
                break;
            case METHOD:
            default:
                break;
        }
        return keyBuilder.toString();
    }

    private String getClientIp() {
        ServletRequestAttributes attributes =
                (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
        if (attributes == null) {
            return "unknown";
        }
        HttpServletRequest request = attributes.getRequest();
        String ip = getHeaderIp(request, "X-Forwarded-For");
        if (ip == null) {
            ip = getHeaderIp(request, "Proxy-Client-IP");
        }
        if (ip == null) {
            ip = getHeaderIp(request, "WL-Proxy-Client-IP");
        }
        return ip == null ? request.getRemoteAddr() : ip;
    }

    private String getHeaderIp(HttpServletRequest request, String headerName) {
        String ip = request.getHeader(headerName);
        if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
            return null;
        }
        int commaIndex = ip.indexOf(',');
        return commaIndex > -1 ? ip.substring(0, commaIndex).trim() : ip;
    }

    private String getCurrentUserId() {
        UserDTO user = UserHolder.getUser();
        return user == null ? "anonymous" : String.valueOf(user.getId());
    }
}
