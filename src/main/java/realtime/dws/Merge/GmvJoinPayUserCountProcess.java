package realtime.dws.Merge;


import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import realtime.dws.bean.total.GmvStats;
import realtime.dws.bean.total.PayUserCountStats;
import realtime.dws.bean.total.TradeStatsTotal;

public class GmvJoinPayUserCountProcess
        extends KeyedCoProcessFunction<Long, GmvStats, PayUserCountStats, TradeStatsTotal> {
    private ValueState<GmvStats> gmvState;
    private ValueState<PayUserCountStats> payUserCountState;
    @Override
    public void open(Configuration parameters) throws Exception {

        // 维护两个state
        ValueStateDescriptor<GmvStats> gmvDescriptor = new ValueStateDescriptor<>("gmvDescriptor", GmvStats.class);
        gmvState = getRuntimeContext().getState(gmvDescriptor);
        ValueStateDescriptor<PayUserCountStats> payUserCountDescriptor = new ValueStateDescriptor<>("payUserCountDescriptor", PayUserCountStats.class);
        payUserCountState = getRuntimeContext().getState(payUserCountDescriptor);
    }
    @Override
    public void processElement1(GmvStats gmvStats,
                                Context ctx,
                                Collector<TradeStatsTotal> out) throws Exception {
        // 更新当前gmv值
        gmvState.update(gmvStats);
        // 获取payUserCountState的值
        PayUserCountStats payStats = payUserCountState.value();
        // 如果该值不为空则输出两个的组合值
        if (payStats != null) {
            out.collect(merge(gmvStats, payStats));
            gmvState.clear();
            payUserCountState.clear();
        }
    }

    @Override
    public void processElement2(PayUserCountStats payUserCountStats,
                                Context ctx,
                                Collector<TradeStatsTotal> out) throws Exception {

        payUserCountState.update(payUserCountStats);
        GmvStats gmvStats = gmvState.value();
        if (gmvStats != null) {
            out.collect(merge(gmvStats, payUserCountStats));
            // 清除状态
            gmvState.clear();
            payUserCountState.clear();
        }
    }
    private TradeStatsTotal merge(GmvStats gmvStats, PayUserCountStats payUserCountStats) {
        TradeStatsTotal tradeStatsTotal = new TradeStatsTotal();
        tradeStatsTotal.setWindowStart(gmvStats.getWindowStart());
        tradeStatsTotal.setWindowEnd(gmvStats.getWindowEnd());
        tradeStatsTotal.setGmv(gmvStats.getGmv());
        tradeStatsTotal.setRefundAmount(gmvStats.getRefundAmount());
        tradeStatsTotal.setPayUserCount(payUserCountStats.getPayUserCount());
        return tradeStatsTotal;
    }
}
