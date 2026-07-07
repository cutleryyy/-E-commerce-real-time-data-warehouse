package realtime.dwd.process;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import realtime.bean.OrderStatusLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class OrderStatusDedupProcess extends KeyedProcessFunction<
        Tuple2<Long,String>,
        OrderStatusLog,
        OrderStatusLog> {

    // State
    private ValueState<Long> lastEventTime;

    // Metric
    private transient Counter dirtyCounter;

    // SideOutput
    private final OutputTag<OrderStatusLog> dirtyTag;

    // Logger
    private static final Logger LOG =
            LoggerFactory.getLogger(OrderStatusDedupProcess.class);

    /**
     * 构造方法
     * 主程序把侧输出流Tag传进来
     */
    public OrderStatusDedupProcess(OutputTag<OrderStatusLog> dirtyTag){
        this.dirtyTag = dirtyTag;
    }
    @Override
    public void open(Configuration parameters) throws Exception {

//        StateTtlConfig ttl =
//                StateTtlConfig.newBuilder(
//                                Time.days(10))
//                        .build();
        StateTtlConfig ttl = StateTtlConfig
                .newBuilder(Time.days(1)) //超过 1 天后，该状态将被标记为过期（但不会立即物理删除，而是等待后台清理）
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite) //当状态被创建或更新（写入）时，重置它的过期时间
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired) //状态超期值不再返回，返回null
                .build();
        ValueStateDescriptor<Long> descriptor =
                new ValueStateDescriptor<>(
                        "dedup-state",
                        Long.class);

        descriptor.enableTimeToLive(ttl);

        lastEventTime =
                getRuntimeContext().getState(descriptor);

        dirtyCounter =
                getRuntimeContext()
                        .getMetricGroup()
                        .counter("dirty-order");
    }

    @Override
    public void processElement(
            OrderStatusLog value,
            Context ctx,
            Collector<OrderStatusLog> out) throws Exception {
        // TODO 去重
        Long last = lastEventTime.value();
        if (last != null && value.getEventTime() <= last) {
            // 已经处理过
            return;
        }
        // 初始化
        if (last == null) {
            last = -1L;
        }
        //TODO 脏数据检查
        if (value.getOrderId() == null || value.getStatus() == null || value.getAmount() < 0) {
            ctx.output(dirtyTag, value);
            LOG.warn("Dirty data:{}", value);
            dirtyCounter.inc();
            return;
        }
        // 已处理
        // TODO 字段标准化
        value.setChannel(
                value.getChannel()
                        .trim()
                        .toUpperCase());

        value.setStatus(
                value.getStatus()
                        .trim()
                        .toUpperCase());
        // TODO 增加etl时间
        value.setEtlTime(
                System.currentTimeMillis());

        lastEventTime.update(value.getEventTime());
        out.collect(value);
        // debug日志
        LOG.info("DEDUP key={}, last={}, current={}",
                ctx.getCurrentKey(),
                last,
                value.getEventTime());
    }
}
