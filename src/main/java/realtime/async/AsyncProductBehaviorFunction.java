package realtime.async;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import realtime.dws.bean.product.ProductUserBehaviorWide;
import realtime.dws.bean.user.UserBehavior;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

// 先用简单程序直接导入MySQL表，后续可升级为 Flink CDC 同步
public class AsyncProductBehaviorFunction extends RichAsyncFunction<UserBehavior, ProductUserBehaviorWide> {

    private static final Logger LOG = LoggerFactory.getLogger(AsyncProductBehaviorFunction.class);

    private transient RedisClient redisClient;
    private transient StatefulRedisConnection<String,String> connection;
    private transient RedisAsyncCommands<String,String> async;
    // 构建caffeine缓冲池
    private transient Cache<Long,Map<String,String>> cache;
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

    private final ConcurrentHashMap<Long, CompletableFuture<Map<String,String>>> loading =
            new ConcurrentHashMap<>();
    @Override
    public void asyncInvoke(UserBehavior input,
                            ResultFuture<ProductUserBehaviorWide> resultFuture) {

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


    private ProductUserBehaviorWide buildWide(UserBehavior input,Map<String,String> dimMap){
        ProductUserBehaviorWide wide = new ProductUserBehaviorWide();

        wide.setProductId(input.getProductId());
        wide.setWindowStart(input.getWindowStart());
        wide.setWindowEnd(input.getWindowEnd());
        wide.setCountView(input.getCountView());
        wide.setCountClick(input.getCountClick());
        wide.setCountCart(input.getCountCart());
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
                wide.setPrice(null); // 不参与统计
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