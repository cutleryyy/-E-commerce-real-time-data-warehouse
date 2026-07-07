package realtime.dws.function.aggregate;

import org.apache.flink.api.common.functions.AggregateFunction;
import realtime.dws.bean.base.GmvEvent;
import realtime.dws.bean.base.GmvAccumulator;

public class GmvAggregateFunction
        implements AggregateFunction<GmvEvent, GmvAccumulator,GmvAccumulator> {

    @Override
    public GmvAccumulator createAccumulator() {
        return new GmvAccumulator();
    }
    @Override
    public GmvAccumulator add(GmvEvent event, GmvAccumulator acc) {
        long amount = event.getAmountDelta();
        acc.setGmv(acc.getGmv() + amount);
        if (amount < 0) {
            acc.setRefundAmount(acc.getRefundAmount() - amount);
        }
        return acc;
    };
    @Override
    public GmvAccumulator getResult(GmvAccumulator acc) {
        return acc;
    }

    @Override
    public GmvAccumulator merge(GmvAccumulator a, GmvAccumulator b) {
        a.setGmv(a.getGmv() + b.getGmv());
        return a;
    }
}
