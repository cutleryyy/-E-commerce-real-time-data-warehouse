package realtime.common.util;

import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkEnvCheckpointUtil {
    public static void enableCheckpoint(StreamExecutionEnvironment env){
        // TODO 每60秒做一次Checkpoint
//        env.enableCheckpointing(60_000);
        env.enableCheckpointing(30000);

        env.getCheckpointConfig()
                .setCheckpointTimeout(60000);


        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(15000);
//        //TODO 两次Checkpoint之间至少间隔30秒
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30_000);
//
//        //TODO Checkpoint超时时间
//        env.getCheckpointConfig().setCheckpointTimeout(5 * 60 * 1000);
//
//        //TODO 同一时间只允许一个Checkpoint
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
//
//        //TODO Job取消时保留Checkpoint
//        env.getCheckpointConfig()
//                .setExternalizedCheckpointCleanup(
//                        CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//
//        env.getCheckpointConfig().setCheckpointStorage("file:///opt/programs/realtime-warehouse/flink-checkpoints/");
    }
}
