package realtime.dws.Merge;

import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import realtime.dws.bean.product.ProductPayUserStats;
import realtime.dws.bean.product.ProductTradeStats;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;

public class ProductTradeOrderUserMerge
        extends KeyedCoProcessFunction<Tuple2<Integer, Long>, ProductTradeStats, ProductPayUserStats, ProductTradeStats> {

    private ValueState<ProductTradeStats> tradeState;
    private ValueState<ProductPayUserStats> orderUserState;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<ProductTradeStats> tradeDesc =
                new ValueStateDescriptor<>("tradeState", ProductTradeStats.class);
        tradeState = getRuntimeContext().getState(tradeDesc);

        ValueStateDescriptor<ProductPayUserStats> orderDesc =
                new ValueStateDescriptor<>("orderUserState", ProductPayUserStats.class);
        orderUserState = getRuntimeContext().getState(orderDesc);
    }

    @Override
    public void processElement1(ProductTradeStats trade, Context ctx, Collector<ProductTradeStats> out) throws Exception {
        tradeState.update(trade);
        ProductPayUserStats order = orderUserState.value();
        if (order != null) {
            out.collect(merge(trade, order));
            tradeState.clear();
            orderUserState.clear();
        }
    }

    @Override
    public void processElement2(ProductPayUserStats order, Context ctx, Collector<ProductTradeStats> out) throws Exception {
        orderUserState.update(order);
        ProductTradeStats trade = tradeState.value();
        if (trade != null) {
            out.collect(merge(trade, order));
            tradeState.clear();
            orderUserState.clear();
        }
    }

    private ProductTradeStats merge(ProductTradeStats trade, ProductPayUserStats order) {
        trade.setOrderCount(order.getOrderCount());
        trade.setNewUserCount(order.getNewUserCount());
        trade.setOldUserCount(order.getOldUserCount());
        return trade;
    }
}
