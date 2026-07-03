package com.hmdp.mq;

import cn.hutool.json.JSONUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

import static com.hmdp.utils.RedisConstants.*;

@Slf4j
@Component
public class RocketMQProducerService {

    @Resource
    private RocketMQTemplate rocketMQTemplate;

    /**
     * 发送缓存失效消息，通知所有节点清除本地Caffeine缓存
     */
    public void sendCacheInvalidation(String cacheKey, Long shopId) {
        CacheInvalidationMessage msg = new CacheInvalidationMessage(cacheKey, shopId);
        Message<String> message = MessageBuilder.withPayload(JSONUtil.toJsonStr(msg)).build();
        rocketMQTemplate.syncSend(TOPIC_CACHE_INVALIDATION, message);
        log.debug("发送缓存失效消息: key={}", cacheKey);
    }

    /**
     * 发送秒杀订单消息，异步落库
     */
    public void sendSeckillOrder(Long userId, Long voucherId, Long orderId) {
        SeckillOrderMessage msg = new SeckillOrderMessage(userId, voucherId, orderId);
        Message<String> message = MessageBuilder.withPayload(JSONUtil.toJsonStr(msg)).build();
        rocketMQTemplate.syncSend(TOPIC_SECKILL_ORDER, message);
        log.debug("发送秒杀订单消息: orderId={}", orderId);
    }

    /**
     * 发送订单超时延时消息，超时后取消订单并释放库存
     * 延时等级 14 = 10分钟（如需15分钟需在broker配置文件中自定义messageDelayLevel）
     */
    public void sendOrderTimeout(Long orderId, Long voucherId, Long userId) {
        OrderTimeoutMessage msg = new OrderTimeoutMessage(orderId, voucherId, userId);
        Message<String> message = MessageBuilder.withPayload(JSONUtil.toJsonStr(msg)).build();
        int delayLevel = 14; // 10分钟
        rocketMQTemplate.syncSend(TOPIC_ORDER_TIMEOUT, message, 3000, delayLevel);
        log.debug("发送订单超时延时消息: orderId={}, delayLevel={}", orderId, delayLevel);
    }
}
