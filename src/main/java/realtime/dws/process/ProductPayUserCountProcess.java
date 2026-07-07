package realtime.dws.process;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import realtime.dws.bean.base.GmvEvent;

public class ProductPayUserCountProcess extends KeyedProcessFunction<
        Tuple2<Long,Long>,
        GmvEvent,
        GmvEvent> {
    // 去重
    private MapState<Long,Boolean> mapState;

    @Override
    public void open(Configuration parameters){

        MapStateDescriptor<Long,Boolean> descriptor =
                new MapStateDescriptor<>(
                        "window",
                        Long.class,
                        Boolean.class);

        mapState=getRuntimeContext().getMapState(descriptor);
    }

    @Override
    public void processElement(
            GmvEvent value,
            Context ctx,
            Collector<GmvEvent> out)
            throws Exception {

        long windowId=value.getEventTime()/10000;

        if(!mapState.contains(windowId)){

            mapState.put(windowId,true);

            out.collect(value);
        }
    }
}