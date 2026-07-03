package com.hmdp.mq;

import cn.hutool.json.JSONUtil;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;

import static com.hmdp.utils.RedisConstants.CONSUMER_GROUP_SECKILL;
import static com.hmdp.utils.RedisConstants.TOPIC_SECKILL_ORDER;

/**
 * 秒杀订单消费者
 * 消费RocketMQ秒杀订单消息，异步落库。
 * 数据库唯一索引(user_id, voucher_id)作为最终防重手段。
 * 先插入订单（唯一索引防重），再扣减数据库库存，事务保证原子性。
 */
@Slf4j
@Component
@RocketMQMessageListener(
        topic = TOPIC_SECKILL_ORDER,
        consumerGroup = CONSUMER_GROUP_SECKILL,
        selectorExpression = "*"
)
public class SeckillOrderConsumer implements RocketMQListener<String> {

    @Resource
    private VoucherOrderMapper voucherOrderMapper;

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private RocketMQProducerService rocketMQProducerService;

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void onMessage(String message) {
        SeckillOrderMessage msg = JSONUtil.toBean(message, SeckillOrderMessage.class);
        Long userId = msg.getUserId();
        Long voucherId = msg.getVoucherId();
        Long orderId = msg.getOrderId();

        // 1. 先插入订单，唯一索引防重——重复消费时直接跳过
        VoucherOrder order = new VoucherOrder();
        order.setId(orderId);
        order.setUserId(userId);
        order.setVoucherId(voucherId);
        order.setStatus(1); // 1=未支付
        order.setCreateTime(LocalDateTime.now());
        try {
            voucherOrderMapper.insert(order);
        } catch (DuplicateKeyException e) {
            log.warn("重复消费，唯一索引拦截: userId={}, voucherId={}", userId, voucherId);
            return;
        }

        // 2. 扣减数据库库存（与1在同一事务中，防止DB库存与订单不一致）
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherId)
                .gt("stock", 0)
                .update();
        if (!success) {
            throw new RuntimeException("库存扣减失败: voucherId=" + voucherId);
        }

        // 3. 发送超时延时消息，超时未支付自动取消并释放库存
        rocketMQProducerService.sendOrderTimeout(orderId, voucherId, userId);

        log.debug("秒杀订单创建成功: orderId={}", orderId);
    }
}
