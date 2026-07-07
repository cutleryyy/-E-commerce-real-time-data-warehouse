package realtime.common.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KafkaSourceUtil {

    /**
     * 通用方法：从 Kafka 读取指定 Topic，反序列化为指定的 JavaBean 类型
     *
     * @param env          执行环境
     * @param topic        Kafka Topic 名称
     * @param groupId      消费者组 ID（每个作业唯一）
     * @param targetClass  目标 JavaBean 的 Class（必须有无参构造器）
     * @param <T>          JavaBean 类型
     * @return DataStream<T>
     */
    public static <T> DataStream<T> createKafkaSourceStream(
            StreamExecutionEnvironment env,
            String topic,
            String groupId,
            Class<T> targetClass) {

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("192.168.94.200:29092")
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty("isolation.level", "read_committed")
                .build();

        DataStream<String> stream = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "Kafka Source: " + topic
        );

        // 反序列化为目标类型
        return stream.map(new RichMapFunction<String, T>() {
            private ObjectMapper mapper;

            @Override
            public void open(Configuration parameters) throws Exception {
                mapper = new ObjectMapper();
            }

            @Override
            public T map(String json) throws Exception {
                return mapper.readValue(json, targetClass);
            }
        })
//                .returns(new TypeHint<T>() {});
                .returns(TypeInformation.of(targetClass)); //Flink 类型擦除
    }
}