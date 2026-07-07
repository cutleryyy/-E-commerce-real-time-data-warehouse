package realtime.dwd.job;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import realtime.bean.OrderStatusLog;

import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import realtime.common.util.FlinkEnvCheckpointUtil;
import realtime.common.util.sink.KafkaSinkUtil;
import realtime.dwd.process.OrderStatusDedupProcess;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
public class DwdOrderStatus {
    private static final Logger LOG =
            LoggerFactory.getLogger(DwdOrderStatus.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(3);
        // TODO 事务配置
        FlinkEnvCheckpointUtil.enableCheckpoint(env);
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("192.168.94.200:29092")
                .setTopics("order_status_log")
//                .setGroupId("test-debug-" + System.currentTimeMillis())
                .setGroupId("flink-group-dwd-orderStatus")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty("isolation.level","read_committed") //source端开启事务
                .build();
//        DataStream<String> stream = env.fromSource(source, WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(40)),"flinkDwd_orderStatus");

        DataStream<String> stream =
                env.fromSource(source, WatermarkStrategy.forMonotonousTimestamps(), "kafka");
//        stream.print("RAW");

        DataStream<OrderStatusLog> orderStatusStream = stream
                .map(
                new RichMapFunction<String, OrderStatusLog>() {
                    private ObjectMapper mapper;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        mapper = new ObjectMapper();
                    }
                    @Override
                    public OrderStatusLog map(String s) throws Exception {
                        return mapper.readValue(s, OrderStatusLog.class);
                    }
                }
        );
//        orderStatusStream.print("MAP");
          // State是从RuntimeContext获取的；对 KeyedStream 上的每个 Key 独立处理，可以访问状态、定时器（Timer）和上下文信息。

        // TODO 定义脏数据输出流
        OutputTag<OrderStatusLog> dirtyOutStatus = new OutputTag<OrderStatusLog>("dirty-order-status"){};

        // TODO 根据组合键去重
        KeyedStream<OrderStatusLog, Tuple2<Long, String>> keyedStream =
                orderStatusStream.keyBy(
                        new KeySelector<OrderStatusLog, Tuple2<Long, String>>() {
                            @Override
                            public Tuple2<Long, String> getKey(OrderStatusLog value) {
                                return Tuple2.of(value.getOrderId(), value.getStatus());
                            }
                        }
                );
//        keyedStream.print("keyedStream:");
        // 处理逻辑
        SingleOutputStreamOperator<OrderStatusLog> streamOperator = keyedStream
                .process(new OrderStatusDedupProcess(dirtyOutStatus));

//        streamOperator.print("ONLY PROCESS RESULT");

        // 流分开输出
        DataStream<OrderStatusLog> validStream = streamOperator;

        DataStream<OrderStatusLog> dirtyStream = streamOperator.getSideOutput(dirtyOutStatus);
        // TODO 构建kafkaSink
        KafkaSink<OrderStatusLog> sink = KafkaSinkUtil.createSink(
                "dwd_order_status",
                OrderStatusLog.class,
                OrderStatusLog::getOrderId,
                "dwd_order_status_tx_"
        );
        // TODO 脏数据输出topic
        KafkaSink<OrderStatusLog> dirtyOrderStatusSink = KafkaSinkUtil.createSink(
                "dwd_dirty_order_status",
                OrderStatusLog.class,
                OrderStatusLog::getOrderId,
                "dwd_dirty_order_status_tx_"
        );
        validStream.sinkTo(sink);
        dirtyStream.sinkTo(dirtyOrderStatusSink);

        validStream.print();

        env.execute("DwdOrderStatus");
    }
}
