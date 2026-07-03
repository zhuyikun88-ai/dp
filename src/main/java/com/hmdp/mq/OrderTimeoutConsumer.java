package com.hmdp.mq;

import cn.hutool.json.JSONUtil;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;

import static com.hmdp.utils.RedisConstants.CONSUMER_GROUP_TIMEOUT;
import static com.hmdp.utils.RedisConstants.TOPIC_ORDER_TIMEOUT;

/**
 * 订单超时消费者
 * 接收RocketMQ延时消息，检查订单支付状态。若超时未支付，取消订单并释放库存。
 * 使用事务保证订单取消与库存恢复的原子性。
 */
@Slf4j
@Component
@RocketMQMessageListener(
        topic = TOPIC_ORDER_TIMEOUT,
        consumerGroup = CONSUMER_GROUP_TIMEOUT,
        selectorExpression = "*"
)
public class OrderTimeoutConsumer implements RocketMQListener<String> {

    @Resource
    private VoucherOrderMapper voucherOrderMapper;

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void onMessage(String message) {
        OrderTimeoutMessage msg = JSONUtil.toBean(message, OrderTimeoutMessage.class);
        Long orderId = msg.getOrderId();
        Long voucherId = msg.getVoucherId();

        // 1. 查询订单当前状态
        VoucherOrder order = voucherOrderMapper.selectById(orderId);
        if (order == null) {
            log.warn("订单不存在: orderId={}", orderId);
            return;
        }

        // 2. 仅处理未支付订单（status=1），已支付/已取消的跳过
        if (order.getStatus() != 1) {
            log.debug("订单已处理，跳过: orderId={}, status={}", orderId, order.getStatus());
            return;
        }

        // 3. 取消订单
        order.setStatus(4); // 4=已取消
        order.setUpdateTime(LocalDateTime.now());
        voucherOrderMapper.updateById(order);

        // 4. 恢复库存
        seckillVoucherService.update()
                .setSql("stock = stock + 1")
                .eq("voucher_id", voucherId)
                .update();

        log.info("超时订单已取消，库存已恢复: orderId={}, voucherId={}", orderId, voucherId);
    }
}
