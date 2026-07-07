package com.test;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KafkaToFlink {
    public static void main(String[] args) throws Exception {
//        创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        并行度
        env.setParallelism(1);
//      数据源kafka，接受数据为字符串类型，定义连接、topic、consumer group、从头offset开始
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("192.168.94.200:29092")
                .setTopics("user_behavior")
                .setGroupId("flink-group")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setStartingOffsets(OffsetsInitializer.earliest())
                .build();

        DataStream<String> stream =env.fromSource(source, WatermarkStrategy.noWatermarks(),"kafka source");
        stream.print();
        env.execute("kafka to flink test job");
    }
}
