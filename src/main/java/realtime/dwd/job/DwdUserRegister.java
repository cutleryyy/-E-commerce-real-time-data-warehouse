package realtime.dwd.job;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
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
import realtime.bean.UserClickLog;
import realtime.bean.UserRegisterLog;
import realtime.common.util.FlinkEnvCheckpointUtil;
import realtime.common.util.KafkaSourceUtil;
import realtime.common.util.sink.KafkaSinkUtil;

public class DwdUserRegister {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkEnvCheckpointUtil.enableCheckpoint(env);
        env.setParallelism(3);

        DataStream<UserRegisterLog> stream = KafkaSourceUtil.createKafkaSourceStream(
                env,
                "user_register_log",
                "flink-group-dwd-userRegister",
                UserRegisterLog.class
        );

        // 按照字段去重
        KeyedStream keyedStream = stream.keyBy(UserRegisterLog::getUserId);
//        keyedStream.print("去重数据：");
        // 脏数据流
        OutputTag<UserRegisterLog> dirtyUserRegisterLog = new OutputTag<UserRegisterLog>("dirtyUserRegisterLog"){};
        SingleOutputStreamOperator<UserRegisterLog> outputStreamOperator = keyedStream.process(
                new KeyedProcessFunction<Long, UserRegisterLog, UserRegisterLog>() {
                    private ValueState<Boolean> seen;

                    // TTL
                    StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.days(30))
                            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                            .build();



                    // 初始化一次连接配置
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<Boolean> desc = new ValueStateDescriptor<>("register_dedup", Boolean.class);
                        desc.enableTimeToLive(ttlConfig);

                        seen = getRuntimeContext().getState(desc);
                    }
                    // 定义处理逻辑
                    @Override
                    public void processElement(UserRegisterLog value,
                                               Context ctx,
                                               Collector<UserRegisterLog> out) throws Exception {

                        // 1. 脏数据判断
                        if (value.getUserId() == null
                                || value.getUserName() == null
                                || value.getUserName().length() < 2
                                || value.getAge() < 0) {

                            ctx.output(dirtyUserRegisterLog, value);
                            return;
                        }

                        // 2. 去重判断
                        Boolean flag = seen.value();

                        if (flag != null && flag) {
                            return;
                        }

                        // 3. 首次出现，标记为true
                        seen.update(true);

                        out.collect(value);
                    }
                }
        );
        DataStream<UserRegisterLog> mainStream = outputStreamOperator;
        DataStream<UserRegisterLog> userRegisterDirtyStream = outputStreamOperator.getSideOutput(dirtyUserRegisterLog);

        KafkaSink<UserRegisterLog> sink = KafkaSinkUtil.createSink(
                "dwd_user_register",
                UserRegisterLog.class,
                UserRegisterLog::getUserId,
                "dwd_user_register_id_"
        );

        KafkaSink<UserRegisterLog> dirtSink = KafkaSinkUtil.createSink(
                "dwd_user_register",
                UserRegisterLog.class,
                UserRegisterLog::getUserId,
                "dwd_dirty_user_register_id_"
        );
//
        mainStream.sinkTo(sink);
        userRegisterDirtyStream.sinkTo(dirtSink);
        mainStream.print("mainStream:");
        env.execute("DwdUserRegister");
    }
}