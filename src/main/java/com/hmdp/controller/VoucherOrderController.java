package com.hmdp.controller;


import com.hmdp.dto.Result;
import com.hmdp.limiter.annotation.RateLimiter;
import com.hmdp.service.IVoucherOrderService;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;

/**
 * <p>
 * 前端控制器
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@RestController
@RequestMapping("/voucher-order")
public class VoucherOrderController {

    @Resource
    private IVoucherOrderService voucherOrderService;

//    @GetMapping("seckill/{id}")
    @PostMapping("seckill/{id}")
    @RateLimiter(
            key = "seckill:global",
            window = 10,
            limit = 500,
            message = "秒杀活动太火爆，请稍后再试",
            type = RateLimiter.LimitType.METHOD
    )
    public Result seckillVoucher(@PathVariable("id") Long voucherId) {
//        return Result.ok();
        return voucherOrderService.seckillVoucher(voucherId);
    }
}
