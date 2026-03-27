package com.hmdp.dto;

import lombok.Data;

import java.io.Serializable;

@Data
public class SeckillOrderMessage implements Serializable {
    private Long outboxId;
    private Long orderId;
    private Long userId;
    private Long voucherId;
}
