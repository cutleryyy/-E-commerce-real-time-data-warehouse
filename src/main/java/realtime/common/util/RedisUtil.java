package realtime.common.util;


import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.concurrent.ExecutorService;

public class RedisUtil {
    // 连接池，减少连接访问频率
    private static JedisPool jedisPool;
//    类加载时初始化（只执行一次）；顺序是：类加载 → static block → 初始化连接池
    static {
    JedisPoolConfig config = new JedisPoolConfig();
    config.setMaxTotal(50);
    config.setMaxIdle(10);
        jedisPool = new JedisPool(
                config,
                "192.168.94.200",
                6379
        );
    }

    // static的意义
//    全 JVM 只初始化一次 ，Class加载时初始化；所有 Flink task 共享连接池；避免连接爆炸
    public static Jedis getJedis() {
        return jedisPool.getResource();
    }
}

//public class RedisUtil {
//    private JedisPool jedisPool;
//
//    public RedisUtil() {
//        jedisPool = new JedisPool("192.168.94.200", 6379);
//    }
//}
//每次 new RedisUtil：都会创建一个新的 JedisPool，每个 Task / 每个算子实例都会创建一套连接池，Redis 连接数瞬间爆炸
