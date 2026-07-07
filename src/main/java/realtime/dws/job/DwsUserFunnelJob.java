package realtime.dws.job;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import realtime.dws.bean.user.UserFunnelStats;
import realtime.bean.OrderPaidLog;
import realtime.bean.UserRegisterLog;
import realtime.common.util.FlinkEnvCheckpointUtil;
import realtime.common.util.KafkaSourceUtil;
import realtime.common.util.sink.KafkaSinkUtil;
import realtime.dws.bean.user.UserFunnelResult;

import java.time.Duration;

public class DwsUserFunnelJob {
    public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(3);
    FlinkEnvCheckpointUtil.enableCheckpoint(env);
    // 输入两条流
    DataStream<UserRegisterLog> registerStream = KafkaSourceUtil.createKafkaSourceStream(
            env,
            "dwd_user_register",
            "flink-group-dws-userRegisterFunnel",
            UserRegisterLog.class
    );
    DataStream<OrderPaidLog> orderStream  = KafkaSourceUtil.createKafkaSourceStream(
            env,
                "dwd_order_paid",
                "flink-group-dws-orderPaidFunnel",
                OrderPaidLog.class
        );
        SingleOutputStreamOperator<UserRegisterLog> userRegisterStream = registerStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<UserRegisterLog>forBoundedOutOfOrderness(Duration.ofMinutes(2)) // 允许5秒乱序
                                .withTimestampAssigner(((userRegisterLog, timestamp)
                                        -> userRegisterLog.getEventTime()))
                                .withIdleness(Duration.ofSeconds(30)) // 如果30秒没有数据则不参与窗口计算
                );
        SingleOutputStreamOperator<OrderPaidLog> orderPaidStream = orderStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<OrderPaidLog>forBoundedOutOfOrderness(Duration.ofMinutes(2)) // 允许5秒乱序
                                .withTimestampAssigner(((orderPaidLog, timestamp)
                                        -> orderPaidLog.getEventTime()))
//                                .withIdleness(Duration.ofSeconds(30)) // 如果30秒没有数据则不参与窗口计算
                );


    // 根据用户id 进行keyby
        KeyedStream<UserRegisterLog, Long> registerKeyed = userRegisterStream.keyBy(UserRegisterLog::getUserId);

        KeyedStream<OrderPaidLog, Long> orderKeyed = orderPaidStream.keyBy(OrderPaidLog::getUserId);


        // join
        DataStream<UserFunnelResult> resultStream =
                registerKeyed.connect(orderKeyed)
                        .process(new KeyedCoProcessFunction<Long,
                                                        UserRegisterLog,
                                                        OrderPaidLog,
                                                        UserFunnelResult>() {
                            // 维护注册时间状态
                            private ValueState<Long> registerState;

                            @Override
                            public void open(Configuration parameters) {
                                ValueStateDescriptor<Long> desc =
                                        new ValueStateDescriptor<>("registerTime", Long.class);
                                registerState = getRuntimeContext().getState(desc);
                            }

                            @Override
                            public void processElement1(UserRegisterLog reg,
                                                        Context ctx,
                                                        Collector<UserFunnelResult> out) throws Exception {
                                registerState.update(reg.getEventTime());
                            }

                            @Override
                            public void processElement2(OrderPaidLog order,
                                                        Context ctx,
                                                        Collector<UserFunnelResult> out) throws Exception {
                                Long registerTime = registerState.value();
                                if (registerTime == null) {
                                    return; // 无注册记录，不参与漏斗，由于生成器中注册流、订单支付流的用户id无直接联系，是随机生成。需要保证计算前提
                                };
                                long orderTime = order.getEventTime();
                                boolean converted = orderTime - registerTime <= 30 * 60 * 1000; //30分钟
                                // 存储字段值到result 输出
                                UserFunnelResult result = new UserFunnelResult();
                                result.setUserId(order.getUserId());
                                result.setRegisterTime(registerTime);
                                result.setOrderTime(orderTime);
                                result.setConverted(converted);
                                out.collect(result);
                            }
                        });
        // 在 UserFunnelJob 中，将 resultStream 按窗口聚合
        DataStream<UserFunnelStats> funnelStatsStream = resultStream
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(10))) // 与 GMV 窗口大小一致
                .allowedLateness(Time.minutes(5))
                .apply(new AllWindowFunction<UserFunnelResult, UserFunnelStats, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<UserFunnelResult> input, Collector<UserFunnelStats> out) {
                        long registerCount = 0;
                        long convertedCount = 0;
                        for (UserFunnelResult r : input) {
                            registerCount++;
                            if (r.isConverted) {
                                convertedCount++;
                            }
                        }
                        UserFunnelStats stats = new UserFunnelStats();
                        stats.setWindowStart(window.getStart());
                        stats.setWindowEnd(window.getEnd());
                        stats.setRegisterCount(registerCount);
                        stats.setConvertedCount(convertedCount);
                        out.collect(stats);
                    }
                });

// 将漏斗汇总写入 Kafka（供下游合并）
        KafkaSink<UserFunnelStats> funnelSink = KafkaSinkUtil.createSink(
                "dws_user_funnel",
                UserFunnelStats.class,
                UserFunnelStats::getWindowStart,
                "dws_user_funnel_tx_"
        );
        funnelStatsStream.sinkTo(funnelSink);
        funnelStatsStream.print();
        env.execute("UserFunnelJob");
    }
}
