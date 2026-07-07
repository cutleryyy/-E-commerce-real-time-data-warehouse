package realtime.dws.job;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import realtime.async.AsyncProductRedisFunction;
import realtime.bean.OrderStatusLog;
import realtime.common.util.FlinkEnvCheckpointUtil;
import realtime.common.util.KafkaSourceUtil;
import realtime.common.util.sink.KafkaSinkUtil;
import realtime.dws.Merge.ProductGmvPayUserMerge;
import realtime.dws.Merge.ProductTradeOrderUserMerge;
import realtime.dws.bean.base.GmvAccumulator;
import realtime.dws.bean.base.GmvEvent;
import realtime.dws.bean.product.ProductGmvStats;
import realtime.dws.bean.product.ProductPayUserStats;
import realtime.dws.bean.product.ProductTradeStats;
import realtime.dws.bean.product.TradeProductStatsWide;
import realtime.dws.function.aggregate.GmvAggregateFunction;
import realtime.dws.process.ProductPayUserCountProcess;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class DwsProductTradeJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkEnvCheckpointUtil.enableCheckpoint(env);
        env.setParallelism(3);
        DataStream<OrderStatusLog> orderStatusStream = KafkaSourceUtil.createKafkaSourceStream(
                env,
                "dwd_order_status",
                "flink-group-dws-productTrade",
                OrderStatusLog.class// 唯一 group.id
        );

        SingleOutputStreamOperator<GmvEvent> gmvStream = orderStatusStream
                .flatMap(new FlatMapFunction<OrderStatusLog, GmvEvent>() {
                    @Override
                    public void flatMap(OrderStatusLog value,
                                        Collector<GmvEvent> out) {

                        if ("PAID".equals(value.getStatus())) {

                            out.collect(
                                    new GmvEvent(
                                            value.getOrderId(),
                                            value.getUserId(),
                                            value.getCategoryId(),
                                            value.getProductId(),
                                            value.getAmount(),
                                            value.getChannel(),
                                            value.getIs_new_user(),
                                            value.getEventTime(),
                                            value.getProvince()

                                    )
                            );
                        } else if ("REFUNDED".equals(value.getStatus())) {
                            GmvEvent event = new GmvEvent();
                            event.setOrderId(value.getOrderId());
                            event.setUserId(value.getUserId());
                            event.setAmountDelta(-value.getAmount());
                            event.setEventTime(value.getEventTime());
                            event.setProvince(value.getProvince());
                            event.setProductId(value.getProductId());
                            event.setCategoryId(value.getCategoryId());
                            event.setChannel(value.getChannel());
                            event.setIsNewUser(value.getIs_new_user());
                            out.collect(event);
                        }
                    }
                });

        // TODO 指定水位线策略，
        SingleOutputStreamOperator<GmvEvent> gmvStreamOperator = gmvStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<GmvEvent>forBoundedOutOfOrderness(Duration.ofMinutes(2)) // 允许5秒乱序
                                .withTimestampAssigner(((gmvEvent, timestamp) ->
                                        gmvEvent.getEventTime()))
                                .withIdleness(Duration.ofSeconds(30)) // 如果30秒没有数据则不参与窗口计算
                );
        //生成GMVevent
        KeyedStream<GmvEvent, Long> gmvEventKeyedStream = gmvStreamOperator.keyBy(GmvEvent::getProductId);
        // payUserCount
        // 1. 组合keyby
        KeyedStream<GmvEvent, Tuple2<Long, Long>> keyed =
                gmvEventKeyedStream.keyBy(new KeySelector<GmvEvent, Tuple2<Long, Long>>() {
                    @Override
                    public Tuple2<Long, Long> getKey(GmvEvent value) throws Exception {
                        return Tuple2.of(value.getProductId(), value.getUserId());
                    }
                });
        // 2. 按照组合键去重
        SingleOutputStreamOperator<GmvEvent> dedupStream = keyed.process(new ProductPayUserCountProcess());
//        dedupStream.print("按照组合键去重："); // 去重流程无误
        DataStream<GmvEvent> paidStream = dedupStream.filter(e -> e.getAmountDelta() > 0);
        // 3. 按照商品统计人数
        SingleOutputStreamOperator<ProductPayUserStats> payUserStream =
                paidStream
                        .keyBy(GmvEvent::getProductId)
                        .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                        .allowedLateness(Time.minutes(5))
                        .aggregate(
                                new AggregateFunction<GmvEvent, Long, Long>() {
                                    @Override
                                    public Long createAccumulator() {
                                        return 0L;
                                    }

                                    @Override
                                    public Long add(GmvEvent value, Long acc) {
                                        return acc + 1;
                                    }

                                    @Override
                                    public Long getResult(Long acc) {
                                        return acc;
                                    }

                                    @Override
                                    public Long merge(Long a, Long b) {
                                        return a + b;
                                    }
                                },
                                new ProcessWindowFunction<Long, ProductPayUserStats, Long, TimeWindow>() {
                                    @Override
                                    public void process(Long productId, Context context,
                                                        Iterable<Long> input, Collector<ProductPayUserStats> out) {

                                        ProductPayUserStats stats = new ProductPayUserStats();
                                        stats.setProductId(productId);
                                        stats.setWindowStart(context.window().getStart());
                                        stats.setWindowEnd(context.window().getEnd());
                                        // payUserCount
                                        stats.setPayUserCount(input.iterator().next());

                                        out.collect(stats);

                                    }
                                });
//        payUserStream.print("按照商品统计人数:");
// TODO 滚动窗口，固定大小，不重叠计算，时间范围10s
        //Gmv
//        int saltFactor = 10; // 盐值数量，根据热点程度调整
//        DataStream<GmvAccumulator> preAggStream = gmvStreamOperator
//                .map(event -> {
//                    int salt = new Random().nextInt(saltFactor);
//                    // 加盐后的 Key：productId_salt
//                    String saltedKey = event.getProductId() + "_" + salt;
//                    return Tuple2.of(saltedKey, event);
//                })
//                .keyBy(t -> t.f0)
//                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
//                .aggregate(new GmvAggregateFunction()); // 预聚合


        SingleOutputStreamOperator<ProductGmvStats> productGmvStream = gmvStreamOperator
                .keyBy(GmvEvent::getProductId)   // Key 类型为 Long
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .allowedLateness(Time.minutes(5))
                .aggregate(
                        new GmvAggregateFunction(),
                        new ProcessWindowFunction<GmvAccumulator, ProductGmvStats, Long, TimeWindow>() {
                            @Override
                            public void process(Long productId,
                                                Context context,
                                                Iterable<GmvAccumulator> elements,
                                                Collector<ProductGmvStats> out) {
                                GmvAccumulator acc = elements.iterator().next();

                                ProductGmvStats stats = new ProductGmvStats();
                                stats.setProductId(productId);                     // 设置商品ID
                                stats.setWindowStart(context.window().getStart());
                                stats.setWindowEnd(context.window().getEnd());
                                stats.setProductGmv(acc.getGmv());                // 商品GMV
                                stats.setRefundAmount(acc.getRefundAmount());     // 商品退款金额
                                out.collect(stats);
                            }
                        }
                );

//        DataStream<ProductGmvStats> finalResult = preAggStream
//                .map(tuple -> {
//                    // 截取原始 productId（去掉后缀）
//                    String originalKey = tuple.f0.split("_")[0];
//                    return Tuple2.of(Integer.parseInt(originalKey), tuple.f1);
//                })
//                .keyBy(t -> t.f0)
//                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
//                .aggregate(
//                        new GmvAggregateFunction(),
//                        new ProcessWindowFunction<GmvAccumulator, ProductGmvStats, Integer, TimeWindow>() {
//                            @Override
//                            public void process(Integer productId,
//                                                Context context,
//                                                Iterable<GmvAccumulator> elements,
//                                                Collector<ProductGmvStats> out) {
//                                GmvAccumulator acc = elements.iterator().next();
//
//                                ProductGmvStats stats = new ProductGmvStats();
//                                stats.setProductId(productId);                     // 设置商品ID
//                                stats.setWindowStart(context.window().getStart());
//                                stats.setWindowEnd(context.window().getEnd());
//                                stats.setProductGmv(acc.getGmv());                // 商品GMV
//                                stats.setRefundAmount(acc.getRefundAmount());     // 商品退款金额
//                                out.collect(stats);
//                            }
//                        }
//                        );


        // TODO 由于该流去重逻辑是orderId+status组合键，所以存在同一个订单窗口多次支付、退款，简单的根据订单状态加减求和是不成立的
        //  新老用户数量统计、订单数量统计
        SingleOutputStreamOperator<ProductPayUserStats> orderUserStats = gmvEventKeyedStream
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .allowedLateness(Time.minutes(5))
                .apply(new WindowFunction<GmvEvent, ProductPayUserStats, Long, TimeWindow>() {
                    @Override
                    public void apply(Long productId, TimeWindow window, Iterable<GmvEvent> input, Collector<ProductPayUserStats> out) {
                        Set<Long> orderSet = new HashSet<>();
                        Set<Long> newUserSet = new HashSet<>();
                        Set<Long> oldUserSet = new HashSet<>();
                        for (GmvEvent event : input) {
                            if (event.getAmountDelta() <= 0) continue; // 跳过退款
                            orderSet.add(event.getOrderId());
                            if (event.getIsNewUser() == 1) {
                                newUserSet.add(event.getUserId());
                            } else {
                                oldUserSet.add(event.getUserId());
                            }
                        }
                        ProductPayUserStats stats = new ProductPayUserStats();
                        stats.setProductId(productId);
                        stats.setWindowStart(window.getStart());
                        stats.setWindowEnd(window.getEnd());
                        stats.setOrderCount((long) orderSet.size());
                        stats.setNewUserCount((long) newUserSet.size());
                        stats.setOldUserCount((long) oldUserSet.size());
                        out.collect(stats);
                    }
                });
// 假设三个流已经准备好：
// productGmvStream: DataStream<ProductGmvStats>
// payUserStream: DataStream<ProductPayUserStats>   // 包含 payUserCount
// orderUserStats: DataStream<ProductPayUserStats> // 包含 orderCount, newUserCount, oldUserCount

// 1. 映射为统一的 Key 类型：Tuple2<Integer, Long> = (productId, windowStart)
        KeyedStream<ProductGmvStats, Tuple2<Long, Long>> gmvKeyed = productGmvStream
                .map(gmv -> {
                    gmv.setWindowStart(gmv.getWindowStart());
                    return gmv;
                })
                .keyBy(new KeySelector<ProductGmvStats, Tuple2<Long, Long>>() {
                    @Override
                    public Tuple2<Long, Long> getKey(ProductGmvStats productGmvStats) throws Exception {
                        return Tuple2.of(productGmvStats.getProductId(), productGmvStats.getWindowStart());
                    }
                });

        KeyedStream<ProductPayUserStats, Tuple2<Long, Long>> payKeyed = payUserStream
                .map(pay -> {
                    pay.setWindowStart(pay.getWindowStart());
                    return pay;
                })
                .keyBy(new KeySelector<ProductPayUserStats, Tuple2<Long, Long>>() {
                    @Override
                    public Tuple2<Long, Long> getKey(ProductPayUserStats productPayUserStats) throws Exception {
                        return Tuple2.of(productPayUserStats.getProductId(), productPayUserStats.getWindowStart());
                    }
                });

        KeyedStream<ProductPayUserStats, Tuple2<Long, Long>> orderKeyed = orderUserStats
                .map(order -> {
                    order.setWindowStart(order.getWindowStart());
                    return order;
                })
                .keyBy(new KeySelector<ProductPayUserStats, Tuple2<Long, Long>>() {
                    @Override
                    public Tuple2<Long, Long> getKey(ProductPayUserStats productPayUserStats) throws Exception {
                        return Tuple2.of(productPayUserStats.getProductId(), productPayUserStats.getWindowStart());
                    }
                });

// 2. 第一步合并：GMV + 支付用户数
        DataStream<ProductTradeStats> partial = gmvKeyed
                .connect(payKeyed)
                .process(new ProductGmvPayUserMerge());

// 3. 第二步合并：部分 TradeStats + 订单用户统计
        KeyedStream<ProductTradeStats, Tuple2<Long, Long>> partialKeyed = partial
                .keyBy(new KeySelector<ProductTradeStats, Tuple2<Long, Long>>() {
                    @Override
                    public Tuple2<Long, Long> getKey(ProductTradeStats productTradeStats) throws Exception {
                        return Tuple2.of(productTradeStats.getProductId(), productTradeStats.getWindowStart());
                    }
                });
        DataStream<ProductTradeStats> finalResult = partialKeyed
                .connect(orderKeyed)
                .process(new ProductTradeOrderUserMerge());


        DataStream<TradeProductStatsWide> productWideStream = AsyncDataStream.unorderedWait(
                finalResult, // 输入
                new AsyncProductRedisFunction(),
                30,
                TimeUnit.SECONDS,
                100
        );

        KafkaSink<TradeProductStatsWide> sink = KafkaSinkUtil.createSink(
                "dws_product_trade_wide",
                TradeProductStatsWide.class,
                TradeProductStatsWide::getProductId,
                "dws_product_trade_wide_tx_"
        );
        productWideStream.sinkTo(sink);

        productWideStream.print("最终商品交易统计：");
        env.execute("DwsProductTrade");
    }
}
