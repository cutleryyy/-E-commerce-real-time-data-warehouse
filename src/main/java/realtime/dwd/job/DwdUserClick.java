package realtime.dwd.job;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
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
import realtime.bean.OrderStatusLog;
import realtime.bean.UserClickLog;
import realtime.common.util.FlinkEnvCheckpointUtil;
import realtime.common.util.KafkaSourceUtil;
import realtime.common.util.sink.KafkaSinkUtil;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Properties;

public class DwdUserClick {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
//        FlinkEnvCheckpointUtil.enableCheckpoint(env);
        DataStream<UserClickLog> stream = KafkaSourceUtil.createKafkaSourceStream(
                env,
                "user_click_log",
                "flink-group-dwd-userClick",
                UserClickLog.class
        );
        OutputTag<UserClickLog> dirtyOutputStream = new OutputTag<UserClickLog>("dirty-userClick"){};


        KeyedStream<UserClickLog,Long> keyedStream = stream.keyBy(UserClickLog::getEventTime);
        SingleOutputStreamOperator<UserClickLog> outputStreamOperator =
                keyedStream.process(
                new KeyedProcessFunction<Long, UserClickLog, UserClickLog>() {
                    private ValueState<UserClickLog> state;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<UserClickLog> descriptor = new ValueStateDescriptor<UserClickLog>("DwdUserClick",UserClickLog.class);
                        state = getRuntimeContext().getState(descriptor);
                    }
                    @Override
                    public void processElement(UserClickLog value, Context ctx, Collector<UserClickLog> out) throws Exception {
                        UserClickLog oldState = state.value();
                        UserClickLog newState = value;

                        if (oldState == null){
                            state.update(value);
                            out.collect(value);
                            return;
                        }

                        if (oldState.getUserId() ==null || oldState.getProductId()<0|| oldState.getEventTime() == null){
                            ctx.output(dirtyOutputStream,value);
                        }
                    }
                });
        DataStream<UserClickLog> mainStream = outputStreamOperator;
        DataStream<UserClickLog> dirtStreamUserClick = outputStreamOperator.getSideOutput(dirtyOutputStream);

        KafkaSink<UserClickLog> sink = KafkaSinkUtil.createSink(
                "dwd_user_click",
                UserClickLog.class,
                UserClickLog::getUserId,
                "dwd_user_click_tx_"
        );
        KafkaSink<UserClickLog> dirtySink = KafkaSinkUtil.createSink(
                "dwd_dirty_user_click",
                UserClickLog.class,
                UserClickLog::getUserId,
                "dwd_dirty_user_click_tx_"
        );
        // 写入kafka
        mainStream.sinkTo(sink);
        dirtStreamUserClick.sinkTo(sink);
        mainStream.print();
        env.execute("DwdUserClick");
    }
}
