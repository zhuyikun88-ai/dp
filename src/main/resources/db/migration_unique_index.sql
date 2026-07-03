-- 秒杀订单表：添加用户+优惠券唯一索引，防止重复消费
ALTER TABLE tb_voucher_order ADD UNIQUE INDEX uk_user_voucher (user_id, voucher_id);
