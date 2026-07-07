package realtime.dws.Merge;


import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import realtime.dws.bean.base.OrderUserStats;
import realtime.dws.bean.total.TradeStatsTotal;

public class TradeStatsJoinProcess
        extends KeyedCoProcessFunction<Long, TradeStatsTotal, OrderUserStats, TradeStatsTotal> {
    private ValueState<TradeStatsTotal> tradeState;
    private ValueState<OrderUserStats> orderState;
    @Override
    public void open(Configuration parameters) throws Exception {

        // 维护两个state
        ValueStateDescriptor<TradeStatsTotal> tradeDescriptor = new ValueStateDescriptor<>("tradeDescriptor", TradeStatsTotal.class);
        ValueStateDescriptor<OrderUserStats> orderDescriptor = new ValueStateDescriptor<>("orderDescriptor", OrderUserStats.class);
        orderState = getRuntimeContext().getState(orderDescriptor);
        tradeState = getRuntimeContext().getState(tradeDescriptor);
    }

    @Override
    public void processElement1(TradeStatsTotal tradeStatsTotal,
                                Context ctx,
                                Collector<TradeStatsTotal> out) throws Exception {
        // 更新当前gmv值
        tradeState.update(tradeStatsTotal);
        OrderUserStats orderStats = orderState.value();
        // 如果该值不为空则输出两个的组合值
        if (orderStats != null) {
            out.collect(merge(tradeStatsTotal, orderStats));
            tradeState.clear();
            orderState.clear();
        }
    }

    @Override
    public void processElement2(OrderUserStats orderUserStats,
                                Context ctx,
                                Collector<TradeStatsTotal> out) throws Exception {

        orderState.update(orderUserStats);
        TradeStatsTotal tradeStatsTotal = tradeState.value();
        if (tradeStatsTotal != null) {
            out.collect(merge(tradeStatsTotal, orderUserStats));
            // 清除状态
            orderState.clear();
            tradeState.clear();
        }
    }

    private TradeStatsTotal merge(TradeStatsTotal tradeStatsTotal, OrderUserStats orderUserStats) {
        TradeStatsTotal result = new TradeStatsTotal();
        result.setWindowStart(tradeStatsTotal.getWindowStart());
        result.setWindowEnd(tradeStatsTotal.getWindowEnd());
        result.setGmv(tradeStatsTotal.getGmv());
        result.setRefundAmount(tradeStatsTotal.getRefundAmount());
        result.setOrderCount(orderUserStats.getOrderCount());
        result.setPayUserCount(tradeStatsTotal.getPayUserCount());
        result.setNewUserCount(orderUserStats.getNewUserCount());
        result.setOldUserCount(orderUserStats.getOldUserCount());
        return result;
    }

}
