package realtime.dws.Merge;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.tuple.Tuple2;
import realtime.dws.bean.product.ProductGmvStats;
import realtime.dws.bean.product.ProductPayUserStats;
import realtime.dws.bean.product.ProductTradeStats;

public class ProductGmvPayUserMerge
        extends KeyedCoProcessFunction<Tuple2<Integer, Long>, ProductGmvStats, ProductPayUserStats, ProductTradeStats> {

    private ValueState<ProductGmvStats> gmvState;
    private ValueState<ProductPayUserStats> payUserState;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<ProductGmvStats> gmvDesc =
                new ValueStateDescriptor<>("gmvState", ProductGmvStats.class);
        gmvState = getRuntimeContext().getState(gmvDesc);

        ValueStateDescriptor<ProductPayUserStats> payDesc =
                new ValueStateDescriptor<>("payUserState", ProductPayUserStats.class);
        payUserState = getRuntimeContext().getState(payDesc);
    }

    @Override
    public void processElement1(ProductGmvStats gmv, Context ctx, Collector<ProductTradeStats> out) throws Exception {
        gmvState.update(gmv);
        ProductPayUserStats pay = payUserState.value();
        if (pay != null) {
            out.collect(merge(gmv, pay));
            gmvState.clear();
            payUserState.clear();
        }
    }

    @Override
    public void processElement2(ProductPayUserStats pay, Context ctx, Collector<ProductTradeStats> out) throws Exception {
        payUserState.update(pay);
        ProductGmvStats gmv = gmvState.value();
        if (gmv != null) {
            out.collect(merge(gmv, pay));
            gmvState.clear();
            payUserState.clear();
        }
    }

    private ProductTradeStats merge(ProductGmvStats gmv, ProductPayUserStats pay) {
        ProductTradeStats stats = new ProductTradeStats();
        stats.setProductId(gmv.getProductId());
        stats.setWindowStart(gmv.getWindowStart());
        stats.setWindowEnd(gmv.getWindowEnd());
        stats.setProductGmv(gmv.getProductGmv());
        stats.setRefundAmount(gmv.getRefundAmount());
        stats.setPayUserCount(pay.getPayUserCount());
        // 其他字段暂设为 null 或 0，后续合并
        return stats;
    }
}