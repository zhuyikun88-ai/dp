package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.github.benmanes.caffeine.cache.Cache;
import com.hmdp.utils.CacheClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.CACHE_NULL_TTL;

/**
 * Redis + Caffeine 二级缓存服务
 * L1: Caffeine 本地缓存（超高QPS，微秒级延迟）
 * L2: Redis 分布式缓存（高QPS，毫秒级延迟）
 * L3: MySQL 数据库
 */
@Slf4j
@Component
public class TwoLevelCacheService {

    @Resource
    private Cache<String, Object> localCache;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    /**
     * 二级缓存查询：Caffeine -> Redis -> DB
     */
    public <R, ID> R queryWithTwoLevelCache(
            String keyPrefix, ID id, Class<R> type,
            Function<ID, R> dbFallback, Long redisTtl, TimeUnit unit) {

        String key = keyPrefix + id;

        // 1. L1: 查询Caffeine本地缓存
        Object localValue = localCache.getIfPresent(key);
        if (localValue != null) {
            log.debug("Caffeine命中: {}", key);
            return (R) localValue;
        }

        // 2. L2: 查询Redis分布式缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        if (StrUtil.isNotBlank(json)) {
            R r = JSONUtil.toBean(json, type);
            localCache.put(key, r);
            log.debug("Redis命中，回填Caffeine: {}", key);
            return r;
        }

        // 空值缓存穿透处理
        if (json != null) {
            return null;
        }

        // 3. L3: 查询数据库
        R r = dbFallback.apply(id);
        if (r == null) {
            stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }

        // 4. 回写Redis和Caffeine
        String value = JSONUtil.toJsonStr(r);
        stringRedisTemplate.opsForValue().set(key, value, redisTtl, unit);
        localCache.put(key, r);
        log.debug("DB命中，回写Redis+Caffeine: {}", key);

        return r;
    }

    /**
     * 清除指定key的本地缓存（由RocketMQ消息触发）
     */
    public void clearLocalCache(String cacheKey) {
        localCache.invalidate(cacheKey);
        log.debug("清除本地缓存: {}", cacheKey);
    }

    /**
     * 清除指定key的全部缓存（Redis + Caffeine）
     */
    public void clearAllCache(String cacheKey) {
        localCache.invalidate(cacheKey);
        stringRedisTemplate.delete(cacheKey);
        log.debug("清除全部缓存: {}", cacheKey);
    }
}
