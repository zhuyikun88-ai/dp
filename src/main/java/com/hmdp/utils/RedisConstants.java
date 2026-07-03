package com.hmdp.utils;

public class RedisConstants {
    public static final String LOGIN_CODE_KEY = "login:code:";
    public static final Long LOGIN_CODE_TTL = 2L;
    public static final String LOGIN_USER_KEY = "login:token:";
    public static final Long LOGIN_USER_TTL = 36000L;

    public static final Long CACHE_NULL_TTL = 2L;

    public static final Long CACHE_SHOP_TTL = 30L;
    public static final String CACHE_SHOP_KEY = "cache:shop:";

    public static final String LOCK_SHOP_KEY = "lock:shop:";
    public static final Long LOCK_SHOP_TTL = 10L;

    public static final String SECKILL_STOCK_KEY = "seckill:stock:";
    public static final String BLOG_LIKED_KEY = "blog:liked:";
    public static final String FEED_KEY = "feed:";
    public static final String SHOP_GEO_KEY = "shop:geo:";
    public static final String USER_SIGN_KEY = "sign:";

    /** Caffeine本地缓存相关 */
    public static final Long CAFFEINE_SHOP_TTL = 5L;

    /** RocketMQ Topic */
    public static final String TOPIC_CACHE_INVALIDATION = "cache-invalidation-topic";
    public static final String TOPIC_SECKILL_ORDER = "seckill-order-topic";
    public static final String TOPIC_ORDER_TIMEOUT = "order-timeout-topic";

    /** RocketMQ Consumer Group */
    public static final String CONSUMER_GROUP_CACHE = "cache-invalidation-consumer-group";
    public static final String CONSUMER_GROUP_SECKILL = "seckill-order-consumer-group";
    public static final String CONSUMER_GROUP_TIMEOUT = "order-timeout-consumer-group";

    /** 订单超时时间（分钟） */
    public static final Long ORDER_TIMEOUT_MINUTES = 15L;
}
