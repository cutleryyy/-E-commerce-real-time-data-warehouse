package realtime.ads.job;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import realtime.ads.bean.AdsProductFunnelStats;
import realtime.common.util.KafkaSourceUtil;
import realtime.common.util.sink.KafkaSinkUtil;
import realtime.dws.bean.product.ProductUserBehaviorWide;
import realtime.dws.bean.product.TradeProductStatsWide;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;

/**
 * ADS 层：商品转化漏斗
 *
 * 输入流1：dws_product_user_behavior（商品行为宽表 → ProductUserBehaviorWide）
 * 输入流2：dws_product_trade_wide（商品交易宽表 → TradeProductStatsWide）
 * 处理：按 (productId, windowStart) 双流 Join → 计算各环节转化率
 * 输出：ads_product_funnel
 *
 * 漏斗模型：浏览 → 点击 → 加购 → 支付
 */
public class AdsProductFunnelJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

// ==================== 1. 读取双流 ====================

// 行为流（浏览/点击/加购）
        DataStream<ProductUserBehaviorWide> behaviorSource = KafkaSourceUtil.createKafkaSourceStream(
                env,
                "dws_product_user_behavior",
                "flink-group-ads-productFunnel-behavior",
                ProductUserBehaviorWide.class
        );

// 交易流（支付用户数/GMV）
        DataStream<TradeProductStatsWide> tradeSource = KafkaSourceUtil.createKafkaSourceStream(
                env,
                "dws_product_trade_wide",
                "flink-group-ads-productFunnel-trade",
                TradeProductStatsWide.class
        );

// ==================== 2. 分配水位线 ====================

        SingleOutputStreamOperator<ProductUserBehaviorWide> behaviorStream = behaviorSource
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<ProductUserBehaviorWide>forBoundedOutOfOrderness(Duration.ofMinutes(2))
                                .withTimestampAssigner((b, ts) -> b.getWindowStart())
                                .withIdleness(Duration.ofSeconds(30))
                );

        SingleOutputStreamOperator<TradeProductStatsWide> tradeStream = tradeSource
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<TradeProductStatsWide>forBoundedOutOfOrderness(Duration.ofMinutes(2))
                                .withTimestampAssigner((t, ts) -> t.getWindowStart())
                                .withIdleness(Duration.ofSeconds(30))
                );

// ==================== 3. 统一 keyBy ====================
// 两组流都按 (productId, windowStart) 作为 Join key

//        KeyedStream<ProductUserBehaviorWide, Tuple2<Long, Long>> behaviorKeyed = behaviorStream
//                .keyBy(b -> Tuple2.of(b.getProductId(), b.getWindowStart()));
//
//        KeyedStream<TradeProductStatsWide, Tuple2<Long, Long>> tradeKeyed = tradeStream
//                .keyBy(t -> Tuple2.of(t.getProductId(), t.getWindowStart()));
// 类型擦除
        KeyedStream<ProductUserBehaviorWide, Tuple2<Long, Long>> behaviorKeyed = behaviorStream
                .keyBy(new KeySelector<ProductUserBehaviorWide, Tuple2<Long, Long>>() {
                    @Override
                    public Tuple2<Long, Long> getKey(ProductUserBehaviorWide value) throws Exception {
                        return Tuple2.of(value.getProductId(), value.getWindowStart());
                    }
                });

        KeyedStream<TradeProductStatsWide, Tuple2<Long, Long>> tradeKeyed = tradeStream
                .keyBy(new KeySelector<TradeProductStatsWide, Tuple2<Long, Long>>() {
                    @Override
                    public Tuple2<Long, Long> getKey(TradeProductStatsWide value) throws Exception {
                        return Tuple2.of(value.getProductId(), value.getWindowStart());
                    }
                });

// ==================== 4. 双流 Join ====================

        DataStream<AdsProductFunnelStats> funnelStream = behaviorKeyed
                .connect(tradeKeyed)
                .process(new ProductFunnelJoinFunction());

// ==================== 5. 输出 ====================

        funnelStream.print("商品漏斗:");
        KafkaSink<AdsProductFunnelStats> sink = KafkaSinkUtil.createSink(
                "ads_product_funnel",
                AdsProductFunnelStats.class,
                AdsProductFunnelStats::getProductId,
                "ads_product_funnel_tx_"
        );
        funnelStream.sinkTo(sink);

        env.execute("AdsProductFunnelJob");
    }

    // ========================================================================
// 双流 Join ProcessFunction
// ========================================================================
    public static class ProductFunnelJoinFunction
            extends KeyedCoProcessFunction<Tuple2<Long, Long>, ProductUserBehaviorWide, TradeProductStatsWide, AdsProductFunnelStats> {

        // 缓存行为数据：productId_windowStart → ProductUserBehaviorWide
        private ValueState<ProductUserBehaviorWide> behaviorState;
        // 缓存交易数据：productId_windowStart → TradeProductStatsWide
        private ValueState<TradeProductStatsWide> tradeState;

        @Override
        public void open(Configuration parameters) {
            behaviorState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("behaviorState", ProductUserBehaviorWide.class));
            tradeState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("tradeState", TradeProductStatsWide.class));
        }

        /**
         * 处理行为流数据（先到）
         */
        @Override
        public void processElement1(ProductUserBehaviorWide behavior,
                                    Context ctx,
                                    Collector<AdsProductFunnelStats> out) throws Exception {
            behaviorState.update(behavior);

            TradeProductStatsWide trade = tradeState.value();
            if (trade != null) {
                out.collect(mergeAndCompute(behavior, trade));
                behaviorState.clear();
                tradeState.clear();
            }
        }

        /**
         * 处理交易流数据（先到）
         */
        @Override
        public void processElement2(TradeProductStatsWide trade,
                                    Context ctx,
                                    Collector<AdsProductFunnelStats> out) throws Exception {
            tradeState.update(trade);

            ProductUserBehaviorWide behavior = behaviorState.value();
            if (behavior != null) {
                out.collect(mergeAndCompute(behavior, trade));
                behaviorState.clear();
                tradeState.clear();
            }
        }

        /**
         * 合并行为数据 + 交易数据，计算转化率
         */
        private AdsProductFunnelStats mergeAndCompute(ProductUserBehaviorWide behavior,
                                                      TradeProductStatsWide trade) {

// ---- 各环节数据 ----
            long view = getOrDefault(behavior.getCountView());
            long click = getOrDefault(behavior.getCountClick());
            long cart = getOrDefault(behavior.getCountCart());
            long pay = getOrDefault(trade.getPayUserCount());
            long gmv = getOrDefault(trade.getProductGmv());

// ---- 组装输出 ----
            AdsProductFunnelStats result = new AdsProductFunnelStats();
            result.setWindowStart(behavior.getWindowStart());
            result.setWindowEnd(behavior.getWindowEnd());
            result.setProductId(behavior.getProductId());
            result.setProductName(behavior.getProductName());
            result.setCategoryId(behavior.getCategoryId());
            result.setCategoryName(behavior.getCategoryName());
            result.setPrice(behavior.getPrice());

            result.setCountView(view);
            result.setCountClick(click);
            result.setCountCart(cart);
            result.setPayUserCount(pay);
            result.setGmv(gmv);

// ---- 计算转化率（防止除零） ----
            result.setViewToClickRate(
                    view == 0 ? BigDecimal.ZERO :
                            BigDecimal.valueOf(click)
                                    .divide(BigDecimal.valueOf(view), 4, RoundingMode.HALF_UP)
            );
            result.setClickToCartRate(
                    click == 0 ? BigDecimal.ZERO :
                            BigDecimal.valueOf(cart)
                                    .divide(BigDecimal.valueOf(click), 4, RoundingMode.HALF_UP)
            );
            result.setCartToPayRate(
                    cart == 0 ? BigDecimal.ZERO :
                            BigDecimal.valueOf(pay)
                                    .divide(BigDecimal.valueOf(cart), 4, RoundingMode.HALF_UP)
            );
            result.setViewToPayRate(
                    view == 0 ? BigDecimal.ZERO :
                            BigDecimal.valueOf(pay)
                                    .divide(BigDecimal.valueOf(view), 4, RoundingMode.HALF_UP)
            );

            return result;
        }

        private long getOrDefault(Long value) {
            return value == null ? 0L : value;
        }
    }
}