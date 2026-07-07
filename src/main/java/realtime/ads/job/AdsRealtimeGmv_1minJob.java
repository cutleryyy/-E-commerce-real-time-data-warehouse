package realtime.ads.job;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import realtime.ads.aggregate.GmvAgg;
import realtime.ads.bean.AdsGmvStats;
import realtime.ads.process.GmvProcess;
import realtime.common.util.KafkaSourceUtil;
import realtime.common.util.sink.KafkaSinkUtil;
import java.time.Duration;

public class AdsRealtimeGmv_1minJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        // 1. 读取 GMV 统计流（DWS 层输出）
        DataStream<AdsGmvStats> source = KafkaSourceUtil.createKafkaSourceStream(
                env,
                "dws_order_trade",
                "flink-group-ads-orderTotalGmv",
                AdsGmvStats.class
        );

        SingleOutputStreamOperator<AdsGmvStats> adsGmvStatsStream = source
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<AdsGmvStats>forBoundedOutOfOrderness(Duration.ofMinutes(2))
                                .withTimestampAssigner((ads, ts) -> ads.getWindowStart())
                                .withIdleness(Duration.ofSeconds(30))
                );

        // 2. 对 GMV 流进行二次聚合（1分钟窗口，累加）
        KeyedStream<AdsGmvStats, String> keyedStream = adsGmvStatsStream.keyBy(x -> "GMV");
        DataStream<AdsGmvStats> gmvStatsStream = keyedStream
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .allowedLateness(Time.minutes(5))
                .aggregate(new GmvAgg(), new GmvProcess());

//        // 3. 读取漏斗汇总流
//        DataStream<UserFunnelStats> funnelStatsStream = KafkaSourceUtil.createKafkaSourceStream(
//                env,
//                "dws_user_funnel",
//                "flink-group-dws-funnel-merge",
//                UserFunnelStats.class
//        );

        // 4. 按窗口起始时间 keyBy，进行双流合并
//        KeyedStream<AdsGmvStats, Long> gmvKeyed = gmvStatsStream.keyBy(AdsGmvStats::getWindowStart);
//        KeyedStream<UserFunnelStats, Long> funnelKeyed = funnelStatsStream.keyBy(UserFunnelStats::getWindowStart);

//        DataStream<AdsGmvStats> finalStream = gmvKeyed
//                .connect(funnelKeyed)
//                .process(new KeyedCoProcessFunction<Long, AdsGmvStats, UserFunnelStats, AdsGmvStats>() {
//                    private ValueState<AdsGmvStats> gmvState;
//                    private ValueState<UserFunnelStats> funnelState;
//
//                    @Override
//                    public void open(Configuration parameters) throws Exception {
//                        gmvState = getRuntimeContext().getState(
//                                new ValueStateDescriptor<>("gmvState", AdsGmvStats.class));
//                        funnelState = getRuntimeContext().getState(
//                                new ValueStateDescriptor<>("funnelState", UserFunnelStats.class));
//                    }
//
//                    @Override
//                    public void processElement1(AdsGmvStats gmv,
//                                                Context ctx,
//                                                Collector<AdsGmvStats> out) throws Exception {
//                        gmvState.update(gmv);
//                        UserFunnelStats funnel = funnelState.value();
//                        if (funnel != null) {
//                            out.collect(merge(gmv, funnel));
//                            gmvState.clear();
//                            funnelState.clear();
//                        }
//                    }
//
//                    @Override
//                    public void processElement2(UserFunnelStats funnel,
//                                                Context ctx,
//                                                Collector<AdsGmvStats> out) throws Exception {
//                        funnelState.update(funnel);
//                        AdsGmvStats gmv = gmvState.value();
//                        if (gmv != null) {
//                            out.collect(merge(gmv, funnel));
//                            gmvState.clear();
//                            funnelState.clear();
//                        }
//                    }

//                    private AdsGmvStats merge(AdsGmvStats gmv, UserFunnelStats funnel) {
//                        AdsGmvStats ads = new AdsGmvStats();
//                        // 复制 GMV 指标
//                        ads.setWindowStart(gmv.getWindowStart());
//                        ads.setWindowEnd(gmv.getWindowEnd());
//                        ads.setGmv(gmv.getGmv());
//                        ads.setRefundAmount(gmv.getRefundAmount());
//                        ads.setOrderCount(gmv.getOrderCount());
//                        ads.setPayUserCount(gmv.getPayUserCount());
//                        ads.setNewUserCount(gmv.getNewUserCount());
//                        ads.setOldUserCount(gmv.getOldUserCount());
//                        // 添加漏斗指标
//                        ads.setRegisterCount(funnel.getRegisterCount());
//                        ads.setConvertedCount(funnel.getConvertedCount());
//                        // 计算转化率（注册用户转化为支付用户的比例）
//                        long registerCount = funnel.getRegisterCount() != null ? funnel.getRegisterCount() : 0L;
//                        long convertedCount = funnel.getConvertedCount() != null ? funnel.getConvertedCount() : 0L;
//                        ads.setConversionRate(
//                                registerCount == 0 ? BigDecimal.ZERO :
//                                        BigDecimal.valueOf(convertedCount)
//                                                .divide(BigDecimal.valueOf(registerCount), 4, RoundingMode.HALF_UP)
//                        );
//                        return ads;
//                    }
//                });

        // 5. 输出到 Kafka

        KafkaSink<AdsGmvStats> sink = KafkaSinkUtil.createSink(
                "ads_realtime_gmv_1min",
                AdsGmvStats.class,
                AdsGmvStats::getWindowStart,
                "ads_realtime_gmv_1min_tx_"
        );
        gmvStatsStream.sinkTo(sink);
        gmvStatsStream.print();

        env.execute("AdsRealtimeGmv_1min");
    }
}