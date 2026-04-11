package com.hmdp.limiter.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface RateLimiter {

    /**
     * 限流 key 前缀
     */
    String key() default "rate_limit:";

    /**
     * 时间窗口大小，单位为秒
     */
    int window() default 10;

    /**
     * 时间窗口内允许的请求数
     */
    int limit() default 20;

    /**
     * 超限提示信息
     */
    String message() default "系统繁忙，请稍后再试";

    /**
     * 限流维度
     */
    LimitType type() default LimitType.METHOD;

    enum LimitType {
        /**
         * 按调用方 IP 限流
         */
        IP,
        /**
         * 按用户 ID 限流
         */
        USER,
        /**
         * 按方法限流，可作为全局限流使用
         */
        METHOD
    }
}
