package realtime.ods;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import realtime.bean.OrderPaidLog;
import java.time.Duration;

public class OdsOrderApp {
    public static void main(String[] args) throws Exception {
        // 定义环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(3);
        // 设置数据源
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("192.168.94.200:29092")
                .setTopics("order_paid_log")
                .setGroupId("flink-group_order")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        // 创建数据流
        DataStream<String> stream = env.fromSource(source, WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(30)),"kafka to flink ods");
        // 反序列化
        SingleOutputStreamOperator<OrderPaidLog> orderStream = stream.map(new MapFunction<String, OrderPaidLog>() {
            // 初始ObjectMapper
            private ObjectMapper mapper = new ObjectMapper();
            @Override
            public OrderPaidLog map(String s) throws Exception {
                return mapper.readValue(s,OrderPaidLog.class);
            }
        });
//        orderStream.assignTimestampsAndWatermarks();// 需要解析byte[]为JavaBean后提取业务时间作为

        KeyedStream keyedStream = orderStream.keyBy(OrderPaidLog::getUserId);
        keyedStream.print();
        env.enableCheckpointing(60000); // 每分钟做一次 checkpoint
        env.execute("ODS order_paid_log");
    }

}
