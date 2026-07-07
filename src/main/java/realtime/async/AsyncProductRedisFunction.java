package realtime.async;

import com.fasterxml.jackson.core.io.BigDecimalParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import realtime.dws.bean.product.ProductTradeStats;
import realtime.dws.bean.product.TradeProductStatsWide;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.*;

// 先用简单程序直接导入MySQL表，后续可升级为 Flink CDC 同步
public class AsyncProductRedisFunction extends RichAsyncFunction<ProductTradeStats, TradeProductStatsWide> {

    private static final Logger LOG = LoggerFactory.getLogger(AsyncProductRedisFunction.class);

    private transient RedisClient redisClient;
    private transient StatefulRedisConnection<String,String> connection;
    private transient RedisAsyncCommands<String,String> async;
    // 构建caffeine缓冲池
    private transient Cache<Long,Map<String,String>> cache;
//    private transient Jedis jedis;

    // 避免每一条数据都要建一次mapper
    private static final ObjectMapper MAPPER= new ObjectMapper();
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        redisClient = RedisClient.create("redis://192.168.94.200:6379");
        connection = redisClient.connect();
        async = connection.async();

        cache = Caffeine.newBuilder()
                .maximumSize(10000) // 最大容量
                .expireAfterWrite(30, TimeUnit.MINUTES) // 超出阈值自动清理旧数据
                .recordStats() //命中率统计
                .build();
    }

    private final ConcurrentHashMap<Long, CompletableFuture<Map<String,String>>> loading = new ConcurrentHashMap<>();
/*    @Override
    public void asyncInvoke(ProductTradeStats input,
                            ResultFuture<TradeProductStatsWide> resultFuture) {
        Long productId = input.getProductId();
        // 1.查caffeine本地缓存
        Map<String,String> cacheData = cache.getIfPresent(input.getProductId());
        if (cacheData != null){
            // 本地缓存命中，直接返回其中数据
            resultFuture.complete(Collections.singleton(buildWide(input,cacheData)));
            return;
        }
        CompletableFuture<Map<String,String>> future = loading.get(productId);
//        如果已有请求在查 Redis
        String key = "dim:product:" + input.getProductId();

        // 2. 本地缓存无数据，查redis
        if (future != null) {
            return future; // 等待结果
        }
//        如果没有 → 创建请求
        CompletableFuture<Map<String,String>> newFuture =
                CompletableFuture.supplyAsync(() -> redis.hgetall(key));
        loading.put(productId, newFuture);
//      完成后清理 + 回填缓存
        newFuture.thenAccept(map -> {
            cache.put(productId, map);
            loading.remove(productId);
        });

        // 一次性获取全部数据
        RedisFuture<Map<String, String>> future = async.hgetall(key);

        // 避免初始化时返回空指针
        future.whenComplete((map, ex) -> {
            if (ex != null) {
                // 抛出异常，直接输出原始数据，不进行维度关联
                resultFuture.complete(Collections.singleton(buildWide(input,null)));
                LOG.error("Redis error", ex);
                return;
            }
            if (map == null || map.isEmpty()){
                // redis无数据，输出原始数据，同样不补充维度值
                resultFuture.complete(Collections.singleton(buildWide(input,null)));
                return;
            }

            if (isUpdateEvent) {
                cache.invalidate(productId);
            }
            // 3.redis命中，回填caffeine
            cache.put(productId,map);
            // 4. 构建宽表
            TradeProductStatsWide wide = buildWide(input,map);
            resultFuture.complete(Collections.singleton(wide));
        });

        *//**

    }*/
    @Override
    public void asyncInvoke(ProductTradeStats input,
                            ResultFuture<TradeProductStatsWide> resultFuture) {

        Long productId = input.getProductId();
        String key = "dim:product:" + productId;

        // =========================
        // 1. L1 Cache（Caffeine）
        // =========================
        Map<String, String> cacheData = cache.getIfPresent(productId);
        if (cacheData != null) {
            resultFuture.complete(
                    Collections.singleton(buildWide(input, cacheData))
            );
            return;
        }

        // =========================
        // 2. SingleFlight 防击穿
        // =========================
        CompletableFuture<Map<String, String>> existing = loading.get(productId);

        if (existing != null) {
            existing.whenComplete((map, ex) -> {
                resultFuture.complete(
                        Collections.singleton(buildWide(input, map))
                );
            });
            return;
        }

        // =========================
        // 3. 发起 Redis Async IO（关键点：不要 supplyAsync + get）
        // =========================
        RedisFuture<Map<String, String>> redisFuture = async.hgetall(key);

        CompletableFuture<Map<String, String>> cf = redisFuture.toCompletableFuture();

        loading.put(productId, cf);

        // =========================
        // 4. 完成处理
        // =========================
        cf.whenComplete((map, ex) -> {

            loading.remove(productId);

            if (ex != null || map == null || map.isEmpty()) {
                resultFuture.complete(
                        Collections.singleton(buildWide(input, null))
                );
                return;
            }

            // 回填 L1 cache
            cache.put(productId, map);

            resultFuture.complete(
                    Collections.singleton(buildWide(input, map))
            );
        });
    }

    private TradeProductStatsWide buildWide(ProductTradeStats input,Map<String,String> dimMap){
        TradeProductStatsWide wide = new TradeProductStatsWide();

        wide.setProductId(input.getProductId());
        wide.setWindowStart(input.getWindowStart());
        wide.setWindowEnd(input.getWindowEnd());
        wide.setProductGmv(input.getProductGmv());
        wide.setRefundAmount(input.getRefundAmount());
        wide.setOrderCount(input.getOrderCount());
        wide.setPayUserCount(input.getPayUserCount());
        wide.setNewUserCount(input.getNewUserCount());
        wide.setOldUserCount(input.getOldUserCount());

        // 补充维度字段
        if (dimMap!=null){
            wide.setProductName(dimMap.get("product_name"));
            String categoryId = dimMap.get("category_id");
            String priceStr = dimMap.get("price");
            if (categoryId !=null && !"null".equals(categoryId)){
                wide.setCategoryId(Long.parseLong(categoryId));
            }
            if (priceStr != null && !priceStr.isEmpty()) {
                try {
                    BigDecimal price = new BigDecimal(priceStr);
                    wide.setPrice(price);
                } catch (NumberFormatException e) {
                    // 处理非数字字符串（日志或设默认值）
                    LOG.warn("Invalid price format: {}", priceStr);
                    wide.setPrice(BigDecimal.ZERO); // 或保留 null
                }
            } else {
                wide.setPrice(null); // 或默认值
            }
            wide.setCategoryName(dimMap.get("category_name"));
        }
        return wide;
    };

    @Override
    public void close() throws Exception {
        if(connection !=null){
        connection.close();
        }
        if (redisClient !=null){
        redisClient.shutdown();
        }
        // 避免初始化失败空指针
    }
}