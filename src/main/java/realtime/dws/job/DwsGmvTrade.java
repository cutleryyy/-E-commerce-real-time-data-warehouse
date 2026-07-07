package realtime.dws.job;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import realtime.bean.OrderStatusLog;
import realtime.common.util.FlinkEnvCheckpointUtil;
import realtime.common.util.KafkaSourceUtil;
import realtime.common.util.sink.KafkaSinkUtil;
import realtime.dws.bean.base.GmvAccumulator;
import realtime.dws.bean.base.GmvEvent;
import realtime.dws.bean.base.OrderUserStats;
import realtime.dws.bean.total.GmvStats;
import realtime.dws.bean.total.PayUserCountStats;
import realtime.dws.bean.total.TradeStatsTotal;
import realtime.dws.function.aggregate.GmvAggregateFunction;
import realtime.dws.Merge.GmvJoinPayUserCountProcess;
import realtime.dws.Merge.TradeStatsJoinProcess;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

public class DwsGmvTrade {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        // TODO 事务配置
        FlinkEnvCheckpointUtil.enableCheckpoint(env);
        // 数据源
        DataStream<OrderStatusLog> orderStatusStream = KafkaSourceUtil.createKafkaSourceStream(
                env,
                "dwd_order_status",
                "flink-group-dws-orderGmv",
                OrderStatusLog.class
        );

        DataStream<GmvEvent> gmvStream = orderStatusStream
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
                            event.setIsNewUser(value.getIs_new_user());
                            event.setProvince(value.getProvince());
                            event.setChannel(value.getChannel());
                            event.setProvince(value.getProvince());

                            out.collect(event);
                        }
                    }
                });

        // TODO 指定水位线策略，
        SingleOutputStreamOperator<GmvEvent> gmvEventStreamOperator = gmvStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<GmvEvent>forBoundedOutOfOrderness(Duration.ofMinutes(2)) // 允许5秒乱序
                                .withTimestampAssigner(((gmvEvent, timestamp)
                                        -> gmvEvent.getEventTime()))
                                .withIdleness(Duration.ofSeconds(30)) // 如果30秒没有数据则不参与窗口计算
                );
        // todo userId统计
         KeyedStream<GmvEvent,String> keyedByUser = gmvEventStreamOperator
                .filter(e ->e.getAmountDelta() >0)
                .keyBy(gmvEvent -> "payUser");
        DataStream<PayUserCountStats> payUserCount = keyedByUser
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .allowedLateness(Time.minutes(5))
                .aggregate(
                        new AggregateFunction<GmvEvent, Long, Long>() {
                            @Override
                            public Long createAccumulator() {return 0L;}
                            @Override
                            public Long add(GmvEvent event, Long acc) {return acc +1;}
                            @Override
                            public Long getResult(Long acc) {return acc;}
                            @Override
                            public Long merge(Long a, Long b) {return a +b;}
                        },
                        new ProcessWindowFunction<Long,PayUserCountStats,String,TimeWindow>(){
                            @Override
                            public void process(String s, Context context, Iterable<Long> elements, Collector<PayUserCountStats> out) throws Exception {
                                Long value = elements.iterator().next();
                                PayUserCountStats stats = new PayUserCountStats();
                                stats.setWindowStart(context.window().getStart());
                                stats.setWindowEnd(context.window().getEnd());
                                stats.setPayUserCount(value);
                                out.collect(stats);
                        }
                        });
// TODO 指定为gmv值输出
        KeyedStream<GmvEvent,String> keyed = gmvEventStreamOperator.keyBy(gmvEvent -> "gmv");
//        gmvEventStreamOperator.print("分配Watermark后");    // 确认事件时间正常
        // TODO 滚动窗口，固定大小，不重叠计算，时间范围10s
        DataStream<GmvStats> gmvStateStream = keyed
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .allowedLateness(Time.minutes(5))
                .aggregate(new GmvAggregateFunction(),
                        new ProcessWindowFunction<GmvAccumulator, GmvStats, String, TimeWindow>() {
                            @Override
                            public void process(String key, Context context, Iterable<GmvAccumulator> elements, Collector<GmvStats> out) throws Exception {

                                // 获取累加器结果
                                GmvAccumulator acc = elements.iterator().next();

                                // TODO 最终统计格式
                                GmvStats stats = new GmvStats();
                                stats.setWindowStart(context.window().getStart());
                                stats.setWindowEnd(context.window().getEnd());
                                stats.setGmv(acc.getGmv());
                                stats.setRefundAmount(acc.getRefundAmount());
                                out.collect(stats);
                            }
                        });
//        gmvStateStream.print("GMV");
//        payUserCount.print("USER");
//

        // TODO 由于该流去重逻辑是orderId+status组合键，所有存在同一个订单窗口多次支付、退款，简单的根据订单状态加减求和是不成立的
        //  新老用户数量统计、订单数量统计
        DataStream<GmvEvent> paidStream = gmvEventStreamOperator
                .filter(event -> event.getAmountDelta() > 0); // 只保留正金额（支付）
        DataStream<OrderUserStats> orderUserStats = paidStream
                .filter(e ->e.getAmountDelta() >0)
                .keyBy(gmvEvent -> "orderUser")
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .allowedLateness(Time.minutes(5))
                .apply(new WindowFunction<GmvEvent, OrderUserStats, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<GmvEvent> input, Collector<OrderUserStats> out) throws Exception {
                        Set<Long> orderSet = new HashSet<>();
                        Set<Long> newUserSet = new HashSet<>();
                        Set<Long> oldUserSet = new HashSet<>();
                        for (GmvEvent event: input){
                            orderSet.add(event.getOrderId());
                            if (event.getIsNewUser() == 1){
                                newUserSet.add(event.getUserId());
                            }else {
                                oldUserSet.add(event.getUserId());
                            }
                        }
                        OrderUserStats stats = new OrderUserStats();
                        stats.setWindowStart(window.getStart());
                        stats.setWindowEnd(window.getEnd());
                        stats.setOrderCount((long)orderSet.size());
                        stats.setNewUserCount((long) newUserSet.size());
                        stats.setOldUserCount((long) oldUserSet.size());
                        out.collect(stats);
                    }
                });
        //TODO 根据窗口范围连接gmv\payUserCount\orderUserStats两个流
        // 先keyby
        KeyedStream<PayUserCountStats,Long> userKeyed = payUserCount.keyBy(PayUserCountStats::getWindowStart);
        KeyedStream<GmvStats,Long> gmvKeyed = gmvStateStream.keyBy(GmvStats::getWindowStart);
        KeyedStream<OrderUserStats,Long> orderKeyed = orderUserStats.keyBy(OrderUserStats::getWindowStart);
//
//        gmvKeyed.print("GMV KEYED");
//        userKeyed.print("USER KEYED");

        // 通过数据流API接受连接流
        DataStream<TradeStatsTotal> tradeStats = gmvKeyed
                .connect(userKeyed)
                .process(new GmvJoinPayUserCountProcess());
        // 再用连接后的数据流join orderKeyed
        KeyedStream tradeKeyedStream = tradeStats.keyBy(TradeStatsTotal::getWindowStart);
        DataStream<TradeStatsTotal> tradeResult = tradeKeyedStream
                .connect(orderKeyed)
                .process(new TradeStatsJoinProcess());

        // 统计的是每个窗口时间内的指标
        // 窗口会关闭，滚动到下一个时间
        // 对累计的gmv求和
        /*DataStream<TradeStatsTotal> tradeTotalGmv = tradeResult.keyBy(x -> "all")
                        .process(new KeyedProcessFunction<String, TradeStatsTotal, TradeStatsTotal>() {
                            private ValueState<Long> totalGmv;
                            @Override
                            public void open(Configuration parameters) throws Exception {
                                ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor<>("allTrade", Long.class);
                                totalGmv = getRuntimeContext().getState(descriptor);
                            }
                            @Override
                            public void processElement(TradeStatsTotal value, Context ctx, Collector<TradeStatsTotal> out) throws Exception {
                            Long total = totalGmv.value();

                            if (total ==null){
                                total =0L;
                            }
                            // 结果累加
                            total += value.getGmv();
                            // 更新状态
                            totalGmv.update(total);
                            // 更新gmv字段值
                            value.setGmv(total);
                            // 收集结果
                            out.collect(value);
                            }
                        });
        tradeTotalGmv.print("TradeTotalGmv:");*/

        KafkaSink<TradeStatsTotal> sink = KafkaSinkUtil.createSink(
                "dws_order_trade",
                TradeStatsTotal.class,
                "dws_order_trade_tx_"
        );
        tradeResult.sinkTo(sink);

        tradeResult.print("TRADE:");
        env.execute("dwsGmvJob");
    }
}