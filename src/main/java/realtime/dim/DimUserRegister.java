package realtime.dim;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
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
import realtime.bean.UserRegisterLog;
import realtime.common.util.FlinkEnvCheckpointUtil;
import realtime.common.util.KafkaSourceUtil;
import realtime.common.util.sink.KafkaSinkUtil;

public class DimUserRegister {
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
                    private ValueState<UserRegisterLog> state;

                    // 初始化一次连接配置
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<UserRegisterLog> descriptor = new ValueStateDescriptor<>("DimUserRegister", UserRegisterLog.class);
                        //获取流中当前的数据
                        state = getRuntimeContext().getState(descriptor);
                    }
                    // 定义处理逻辑
                    @Override
                    public void processElement(UserRegisterLog value, Context ctx, Collector<UserRegisterLog> out) throws Exception {
                        UserRegisterLog oldState = state.value();
                        UserRegisterLog newState = value;
                        if (value.getUserId()==null||value.getUserName().length() < 2 || value.getAge() <0){
                            ctx.output(dirtyUserRegisterLog,value);
                            return;
                        }
                        if (oldState == null){
                            state.update(value);
                            out.collect(value);
                            return;
                        }
                        if (state.value().getEventTime() > value.getEventTime()) {
                            state.update(value);
                            out.collect(value);
                        }
                    }
                }
        );
        DataStream<UserRegisterLog> mainStream = outputStreamOperator;
        DataStream<UserRegisterLog> userRegisterDirtyStream = outputStreamOperator.getSideOutput(dirtyUserRegisterLog);

        KafkaSink<UserRegisterLog> sink = KafkaSinkUtil.createSink(
                "dim_user_register",
                UserRegisterLog.class,
                UserRegisterLog::getUserId,
                "dim_user_register_id_"
        );

        KafkaSink<UserRegisterLog> dirtSink = KafkaSinkUtil.createSink(
                "dim_user_register",
                UserRegisterLog.class,
                UserRegisterLog::getUserId,
                "dim_dirty_user_register_id_"
        );
//
        mainStream.sinkTo(sink);
        userRegisterDirtyStream.sinkTo(dirtSink);
        mainStream.print("mainStream:");
//        mainStream.print();

//        写入 MySQL（JDBC Sink）
//        mainStream.addSink(
//                JdbcSink.sink(
//                        "INSERT INTO dim_user (user_id, user_name, age, gender, province, vip_level, register_time) VALUES (?, ?, ?, ?, ?, ?, ?)",
//                        (ps, user) -> {
//                            ps.setLong(1, user.getUserId());
//                            ps.setString(2, user.getUserName());
//                            ps.setInt(3, user.getAge());
//                            ps.setInt(4, user.getGender());
//                            ps.setString(5, user.getProvince());
//                            ps.setInt(6, user.getVipLevel());
//                            ps.setLong(7, user.getWindowStart());
//                        },
//                        JdbcExecutionOptions.builder()
//                                .withBatchSize(1000)
//                                .withBatchIntervalMs(200)
//                                .build(),
//                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
//                                .withUrl("jdbc:mysql://192.168.94.200:3306/realtime_db?useSSL=false&serverTimezone=UTC")
//                                .withDriverName("com.mysql.cj.jdbc.Driver")
//                                .withUsername("root")
//                                .withPassword("root123")
//                                .build()
//                )
//        );
        env.execute("DimUserRegister");
    }
}
