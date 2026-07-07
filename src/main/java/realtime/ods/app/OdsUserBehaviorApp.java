package realtime.ods.app;
import com.test.UserBehavior;
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


public class OdsUserBehaviorApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("192.168.94.200:29092")
                .setTopics("user_behavior")
                .setGroupId("flink-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(),"kafka source");
//        json转JavaBean，MapFunction
        SingleOutputStreamOperator<UserBehavior> userStream = stream.map(
                new MapFunction<String, UserBehavior>() {
//                    Jackson反序列化
                    private ObjectMapper mapper = new ObjectMapper(); // 初始化ObjectMapper
            @Override
                    public UserBehavior map(String json) throws Exception{
                    return mapper.readValue(json,UserBehavior.class);
            }
        });
        KeyedStream keyedStream = userStream.keyBy(UserBehavior::getUserId);
//        keyedStream.countWindow(2);
//        stream.map()
//        userStream.print();
//        ValueState<Long> countState = keyedStream;
        SingleOutputStreamOperator<UserBehavior> buystream = userStream.filter(x -> "purchase".equals(x.getAction()));
        buystream.keyBy(x ->"gmv").process(new GmvProcessFunction()).print("GMV");
//        SingleOutputStreamOperator<Double> amountStream = buystream.map(UserBehavior::getAmount);
//        amountStream.print();

        env.execute("kafka to ods_flink");
    }

}
