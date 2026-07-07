package realtime.dws.job;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import realtime.async.AsyncProductBehaviorFunction;
import realtime.bean.UserClickLog;
import realtime.common.util.FlinkEnvCheckpointUtil;
import realtime.common.util.KafkaSourceUtil;
import realtime.common.util.sink.KafkaSinkUtil;
import realtime.dws.bean.product.ProductUserBehaviorWide;
import realtime.dws.bean.user.UserBehavior;
import realtime.dws.function.aggregate.ProductUserBehaviorAggregateFunction;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class DwsProductBehaviorWideJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkEnvCheckpointUtil.enableCheckpoint(env);
        env.setParallelism(3);

        DataStream<UserClickLog> userClickLogDataStream= KafkaSourceUtil.createKafkaSourceStream(
                env,
                "dwd_user_click",
                "flink-group-dws-ProductUserBehavior",   // 唯一 group.id,
                UserClickLog.class
        );
        SingleOutputStreamOperator<UserClickLog> userClickStream = userClickLogDataStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<UserClickLog>forBoundedOutOfOrderness(Duration.ofMinutes(2)) // 允许5秒乱序
                                .withTimestampAssigner(((userClickLog, timestamp)
                                        -> userClickLog.getEventTime()))
                                .withIdleness(Duration.ofSeconds(30)) // 如果30秒没有数据则不参与窗口计算
                );
        KeyedStream<UserClickLog,Long> keyedStream = userClickStream.keyBy(UserClickLog::getProductId);

        DataStream<UserBehavior> stream = keyedStream
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .allowedLateness(Time.minutes(5))
                .aggregate(
                        new ProductUserBehaviorAggregateFunction(),
                        new ProcessWindowFunction<UserBehavior, UserBehavior, Long, TimeWindow>() {

                            @Override
                            public void process(
                                    Long key,
                                    Context context,
                                    Iterable<UserBehavior> elements,
                                    Collector<UserBehavior> out) {

                                // aggregate已经保证只有1条
                                UserBehavior acc = elements.iterator().next();

                                UserBehavior result = new UserBehavior();

                                result.setProductId(acc.getProductId());
                                result.setCountView(acc.getCountView());
                                result.setCountClick(acc.getCountClick());
                                result.setCountCart(acc.getCountCart());

                                // 补充窗口时间
                                result.setWindowStart(context.window().getStart());
                                result.setWindowEnd(context.window().getEnd());

                                out.collect(result);
                            }
                        }
                );

        DataStream<ProductUserBehaviorWide> productWideStream = AsyncDataStream.unorderedWait( //不用顺序写入，提高吞吐量
                stream, // 输入
                new AsyncProductBehaviorFunction(),
                30,
                TimeUnit.SECONDS,
                100
        );

        KafkaSink<ProductUserBehaviorWide> sink = KafkaSinkUtil.createSink(
                "dws_product_user_behavior",
                ProductUserBehaviorWide.class,
                ProductUserBehaviorWide::getProductId,
                "dws_product_user_behavior_tx_"
        );
        productWideStream.sinkTo(sink);
        productWideStream.print("productBehaviorWideStream:");
        env.execute("DwsProductBehaviorWideJob");
    }
}

