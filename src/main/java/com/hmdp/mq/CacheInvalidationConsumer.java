package com.hmdp.mq;

import cn.hutool.json.JSONUtil;
import com.hmdp.service.impl.TwoLevelCacheService;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

import static com.hmdp.utils.RedisConstants.CONSUMER_GROUP_CACHE;
import static com.hmdp.utils.RedisConstants.TOPIC_CACHE_INVALIDATION;

/**
 * 缓存失效消费者
 * 监听缓存失效消息，清除本节点Caffeine本地缓存，保证多实例缓存一致性
 */
@Slf4j
@Component
@RocketMQMessageListener(
        topic = TOPIC_CACHE_INVALIDATION,
        consumerGroup = CONSUMER_GROUP_CACHE,
        selectorExpression = "*"
)
public class CacheInvalidationConsumer implements RocketMQListener<String> {

    @Resource
    private TwoLevelCacheService twoLevelCacheService;

    @Override
    public void onMessage(String message) {
        CacheInvalidationMessage msg = JSONUtil.toBean(message, CacheInvalidationMessage.class);
        if (msg.getCacheKey() != null) {
            twoLevelCacheService.clearLocalCache(msg.getCacheKey());
            log.debug("缓存失效处理完成: key={}", msg.getCacheKey());
        }
    }
}
