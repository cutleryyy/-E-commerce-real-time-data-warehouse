package realtime.ads.aggregate;

import org.apache.flink.api.common.functions.AggregateFunction;
import realtime.ads.bean.AdsProductRankStats;
import realtime.dws.bean.product.TradeProductStatsWide;

public class ProductRankingAgg implements AggregateFunction<TradeProductStatsWide, TradeProductStatsWide, TradeProductStatsWide> {
    @Override
    public TradeProductStatsWide createAccumulator() {
        TradeProductStatsWide acc = new TradeProductStatsWide();
        acc.setProductGmv(0L);
        acc.setRefundAmount(0L);
        acc.setOrderCount(0L);
        acc.setPayUserCount(0L);
        acc.setNewUserCount(0L);
        acc.setOldUserCount(0L);
        return acc;
    }

    @Override
    public TradeProductStatsWide add(TradeProductStatsWide value, TradeProductStatsWide acc) {
// 累加数值指标
        acc.setProductGmv(acc.getProductGmv() + value.getProductGmv());
        acc.setRefundAmount(acc.getRefundAmount() + value.getRefundAmount());
        acc.setOrderCount(acc.getOrderCount() + value.getOrderCount());
        acc.setPayUserCount(acc.getPayUserCount() + value.getPayUserCount());
        acc.setNewUserCount(acc.getNewUserCount() + value.getNewUserCount());
        acc.setOldUserCount(acc.getOldUserCount() + value.getOldUserCount());

// 携带维度信息（同一商品维度值相同，取第一个即可）
        if (acc.getProductId() == null) {
            acc.setProductId(value.getProductId());
            acc.setProductName(value.getProductName());
            acc.setCategoryId(value.getCategoryId());
            acc.setCategoryName(value.getCategoryName());
            acc.setPrice(value.getPrice());
        }
        return acc;

    }
    @Override
    public TradeProductStatsWide getResult(TradeProductStatsWide acc) {
        return acc;
    }

    @Override
    public TradeProductStatsWide merge(TradeProductStatsWide a, TradeProductStatsWide b) {
        a.setProductGmv(a.getProductGmv() + b.getProductGmv());
        a.setRefundAmount(a.getRefundAmount() + b.getRefundAmount());
        a.setOrderCount(a.getOrderCount() + b.getOrderCount());
        a.setPayUserCount(a.getPayUserCount() + b.getPayUserCount());
        a.setNewUserCount(a.getNewUserCount() + b.getNewUserCount());
        a.setOldUserCount(a.getOldUserCount() + b.getOldUserCount());
// 维度信息取 a 的（已有值）
        return a;
    }
}
