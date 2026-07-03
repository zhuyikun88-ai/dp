package com.hmdp.mq;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class CacheInvalidationMessage implements Serializable {
    private static final long serialVersionUID = 1L;

    /** 缓存key（如 cache:shop:1） */
    private String cacheKey;

    /** 业务ID */
    private Long shopId;
}
