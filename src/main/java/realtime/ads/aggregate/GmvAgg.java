package realtime.ads.aggregate;

import org.apache.flink.api.common.functions.AggregateFunction;
import realtime.ads.bean.AdsGmvStats;
import realtime.dws.bean.base.GmvAccumulator;

public class GmvAgg implements AggregateFunction<AdsGmvStats, GmvAccumulator, GmvAccumulator>  {
    @Override
    public GmvAccumulator createAccumulator() {
        GmvAccumulator acc = new GmvAccumulator();
        acc.setGmv(0L);
        acc.setRefundAmount(0L);
        acc.setOrderCount(0L);
        acc.setPayUserCount(0L);
        acc.setNewUserCount(0L);
        acc.setOldUserCount(0L);

        return acc;
    }

    @Override
    public GmvAccumulator add(AdsGmvStats value, GmvAccumulator acc) {
        acc.add(value.getGmv());
        acc.setRefundAmount(acc.getRefundAmount() + value.getRefundAmount());
        acc.setOrderCount(acc.getOrderCount() + value.getOrderCount());
        acc.setPayUserCount(acc.getPayUserCount() + value.getPayUserCount());
        acc.setNewUserCount(acc.getNewUserCount() + value.getNewUserCount());
        acc.setOldUserCount(acc.getOldUserCount() + value.getOldUserCount());

        return acc;
    }

    @Override
    public GmvAccumulator getResult(GmvAccumulator acc) {
        return acc;
    }

    @Override
    public GmvAccumulator merge(GmvAccumulator a, GmvAccumulator b) {
        // 合并两个累加器
        a.setGmv(a.getGmv() + b.getGmv());
        a.setRefundAmount(a.getRefundAmount() + b.getRefundAmount());
        a.setOrderCount(a.getOrderCount() + b.getOrderCount());
        a.setPayUserCount(a.getPayUserCount() + b.getPayUserCount());
        a.setNewUserCount(a.getNewUserCount() + b.getNewUserCount());
        a.setOldUserCount(a.getOldUserCount() + b.getOldUserCount());
        return a;
    }
}

