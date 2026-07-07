package realtime.dws.function.aggregate;

import org.apache.flink.api.common.functions.AggregateFunction;
import realtime.dws.bean.base.GmvEvent;
import realtime.dws.bean.product.ProductTradeAccumulator;

public class ProductTradeAggregateFunction
        implements AggregateFunction<
                GmvEvent,
        ProductTradeAccumulator,
                ProductTradeAccumulator> {

    @Override
    public ProductTradeAccumulator createAccumulator() {
        return new ProductTradeAccumulator();
    }

    @Override
    public ProductTradeAccumulator add(GmvEvent event,
                                       ProductTradeAccumulator acc) {

        if (event.getAmountDelta() > 0) {

            acc.setGmv(acc.getGmv() + event.getAmountDelta());

            acc.setOrderCount(acc.getOrderCount() + 1);

        } else {

            acc.setRefundAmount(
                    acc.getRefundAmount() + (-event.getAmountDelta()));
        }

        return acc;
    }

    @Override
    public ProductTradeAccumulator getResult(ProductTradeAccumulator acc) {
        return acc;
    }

    @Override
    public ProductTradeAccumulator merge(ProductTradeAccumulator a,
                                         ProductTradeAccumulator b) {

        a.setGmv(a.getGmv() + b.getGmv());

        a.setRefundAmount(
                a.getRefundAmount() + b.getRefundAmount());

        a.setOrderCount(
                a.getOrderCount() + b.getOrderCount());

        return a;
    }
}
