package realtime.dwd.job;

// 去重、字段标准化、时间语义统一、处理乱序
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerConfig;
import realtime.bean.OrderPaidLog;
import realtime.common.util.sink.KafkaSinkUtil;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Properties;

public class DwdOrderPaid {
    public static void main(String[] args) throws Exception {
        // 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // checkpoint存储点
        env.enableCheckpointing(50000);
        //设置并行度
        env.setParallelism(3);

        // 创建KafkaSource:bootstrap、topic、consumergroup、watermark、serialize
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("192.168.94.200:29092")
                .setGroupId("flink-group_dwd-orderPaid")
                .setTopics("order_paid_log")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty("isolation.level","read_committed")
                .build();
        // 创建流
        DataStream<String> stream = env.fromSource(source, WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofMinutes(1)),"flinkDwd_orderPaid");
        // 反序列化为JavaBean对象：SingleOutputStreamOperator,map方法：定义 ObjectMapper
//        DataStream<OrderPaidLog> orderStream = stream.map(new JsonDeserializer<>(OrderPaidLog.class));
        DataStream<OrderPaidLog> orderStream = stream.map(
                new MapFunction<String, OrderPaidLog>() {
                    private ObjectMapper mapper = new ObjectMapper();
                    @Override
                    public OrderPaidLog map(String json) throws Exception{
                        return mapper.readValue(json,OrderPaidLog.class);
                    }
        });

//        KeyedStream<OrderPaidLog, Long> keyed = stream.keyBy(OrderPaidLog::getOrderId);
        KeyedStream keyedStream = orderStream.keyBy(OrderPaidLog::getOrderId);

        // 定义脏数据输出流/侧输出流
        OutputTag<OrderPaidLog> dirtyOut = new OutputTag<OrderPaidLog>("dirty-order"){};

        SingleOutputStreamOperator<OrderPaidLog> streamOperator = keyedStream.process(
            new KeyedProcessFunction<Long,OrderPaidLog,OrderPaidLog>() {
            private ValueState<OrderPaidLog> state;
            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<OrderPaidLog> descriptor = new ValueStateDescriptor<OrderPaidLog>("DwdOrderPaid", OrderPaidLog.class);
                state = getRuntimeContext().getState(descriptor);
            }
            @Override
            public void processElement(OrderPaidLog value, Context ctx, Collector<OrderPaidLog> out) throws Exception {
                //state存储的旧值。value则是新传入的值
                OrderPaidLog oldState = state.value();
                OrderPaidLog newEvent = value;
                // 初始化值
                if (oldState==null){
                    state.update(newEvent);
                    out.collect(newEvent);
                    return;
                }
                Long oldEventTime = oldState.getEventTime();
                Long newEventTime = newEvent.getEventTime();
                //条件判断：如果新传入的时间戳>旧的时间戳才认为这条数据是新的可以传入
                if (newEventTime > oldEventTime) {
                    state.update(newEvent);
                    out.collect(newEvent);
                }
                if (oldState.getAmount() < 0 || oldState.getOrderId() == null|| oldState.getUserId() ==null || oldState.getCategoryId() <0) {
                    ctx.output(dirtyOut,value);
                }
            }
        });
        DataStream<OrderPaidLog> mainStream = streamOperator;
        DataStream<OrderPaidLog> dirtyStream = streamOperator.getSideOutput(dirtyOut);

        // 序列化写入
        SerializationSchema<OrderPaidLog> serializationSchema = new SerializationSchema<OrderPaidLog>() {
            // 初始化objectMapper对象
            private transient ObjectMapper mapper = new ObjectMapper();
            @Override
            public void open(InitializationContext context) throws Exception {
                mapper = new ObjectMapper();
            }
            @Override
            public byte[] serialize(OrderPaidLog element) {
                try {
                    return mapper.writeValueAsBytes(element);
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }
            }
        };
        //定义生产者事务超时时间
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,"600000");

        // sink
        KafkaSink<OrderPaidLog> sink = KafkaSinkUtil.createSink(
                "dwd_order_paid",
                OrderPaidLog.class,
                OrderPaidLog::getOrderId,
                "dwd_order_paid_tx_"
        );
        KafkaSink<OrderPaidLog> dirtySink = KafkaSinkUtil.createSink(
                "dwd_dirty_order_paid",
                OrderPaidLog.class,
                OrderPaidLog::getOrderId,
                "dwd_dirty_order_paid_tx_"
        );
        // 合法数据流写入kafka
        mainStream.sinkTo(sink);
        dirtyStream.sinkTo(dirtySink);
        mainStream.print();
        env.execute("DwdOrderPaid");
    }
}
