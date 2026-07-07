package realtime.dws.process;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import realtime.bean.OrderStatusLog;
import realtime.dws.bean.base.GmvEvent;

public class TradeProcess extends KeyedProcessFunction<Long, OrderStatusLog, GmvEvent> {
//    private ValueState<String> lastStatusState;  // 只存储状态字符串
    private MapState<String, Boolean> lastStatusState;
    private final OutputTag<OrderStatusLog> dirtyTag;
    public TradeProcess(OutputTag<OrderStatusLog> dirtyTag){
        this.dirtyTag = dirtyTag;
    }
    @Override
    public void open(Configuration parameters) throws Exception {
        MapStateDescriptor<String,Boolean> descriptor = new MapStateDescriptor<>("status-state",String.class, Boolean.class);
        lastStatusState = getRuntimeContext().getMapState(descriptor);
    }

    @Override
    // 输入orderstatus，输出gmvevent
    public void processElement(OrderStatusLog value, Context ctx, Collector<GmvEvent> out) throws Exception {
        // TODO 1. 脏数据校验（侧输出可定义）
        if (value.getOrderId() == null || value.getAmount() == null || value.getAmount() < 0) {
            ctx.output(dirtyTag, value);
            return;
        }

        // TODO 3. 如果状态重复（相同状态再次到达），直接忽略
        String status = value.getStatus();
        if (lastStatusState.contains(status)){
            return;
        }
//        lastStatusState.put(status,true);

//        String lastStatus = lastStatusState.value();
//        String newStatus = value.getStatus();
//        if (lastStatus != null && lastStatus.equals(newStatus)) {
//            return;
//        }

        //TODO 4. 根据状态生成 GmvEvent（只处理 PAID 和 REFUNDED）
        GmvEvent event = null;
        if ("PAID".equals(status)) {
            event = new GmvEvent(
                    value.getOrderId(),
                    value.getUserId(),
                    value.getCategoryId(),
                    value.getProductId(),
                    value.getAmount(),
                    value.getChannel(),
                    value.getIs_new_user(),
                    value.getEventTime(),
                    value.getProvince()
            );
        } else if ("REFUNDED".equals(status)) {
            event = new GmvEvent();
            // 退款金额记为负数，便于后续汇总
            event.setOrderId(value.getOrderId());
            event.setUserId(value.getUserId());
            event.setCategoryId(value.getCategoryId());
            event.setProductId(value.getProductId());
            event.setAmountDelta(-value.getAmount());
            event.setChannel(value.getChannel());
            event.setIsNewUser(value.getIs_new_user());
            event.setProvince(value.getProvince());
            event.setEventTime(value.getEventTime());
        }
        //TODO 5. 如果生成了事件，输出
        if (event != null) {
            out.collect(event);
        }
        // TODO 增加etl时间
        value.setEtlTime(
                System.currentTimeMillis());
        //TODO 6. 更新状态（记录本次处理的状态），无论是否输出，只要状态发生变化就更新
        lastStatusState.put(status,true);
    }
}

