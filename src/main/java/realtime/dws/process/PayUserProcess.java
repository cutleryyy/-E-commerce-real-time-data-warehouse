package realtime.dws.process;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import realtime.dws.bean.base.GmvEvent;

public class PayUserProcess extends KeyedProcessFunction<Long, GmvEvent,GmvEvent> {
    // 对一个值多个判断维护
    private MapState<Long,Boolean> windowState;
//    private static final long WINDOW_SIZE = 60_000L;
    @Override
    public void open(Configuration parameters) throws Exception {
//        mapper = new ObjectMapper();
        MapStateDescriptor<Long,Boolean> descriptor = new MapStateDescriptor<>("last-window", Long.class,Boolean.class);
        windowState = getRuntimeContext().getMapState(descriptor);
    }

    @Override
    public void processElement(GmvEvent value, Context ctx, Collector<GmvEvent> out) throws Exception {

        // 当前事件窗口时间属于哪个一分钟窗口，得到序号
        Long currentWindow = value.getEventTime() /10000;

        if (!windowState.contains(currentWindow)){
            windowState.put(currentWindow,true);
            out.collect(value);
        }
    }
}
