package realtime.dws.function.aggregate;

import org.apache.flink.api.common.functions.AggregateFunction;
import realtime.bean.UserClickLog;
import realtime.dws.bean.product.ProductUserBehaviorAcc;
import realtime.dws.bean.user.UserBehavior;

public class ProductUserBehaviorAggregateFunction
        implements AggregateFunction<UserClickLog, ProductUserBehaviorAcc, UserBehavior> {

    @Override
    public ProductUserBehaviorAcc createAccumulator() {
        return new ProductUserBehaviorAcc();
    }

    @Override
    public ProductUserBehaviorAcc add(UserClickLog value, ProductUserBehaviorAcc acc) {
        // 累加
        if ("click".equals(value.getAction())) {
            acc.click++;
        } else if ("view".equals(value.getAction())) {
            acc.view++;
        } else if ("cart".equals(value.getAction())) {
            acc.cart++;
        }

        acc.userId = value.getUserId();
        acc.productId = value.getProductId();

        return acc;
    }

    @Override
    public UserBehavior getResult(ProductUserBehaviorAcc acc) {
        UserBehavior res = new UserBehavior();
        res.setUserId(acc.userId);
        res.setProductId(acc.productId);

        res.setCountClick(acc.click);
        res.setCountView(acc.view);
        res.setCountCart(acc.cart);

        return res;
    }

    @Override
    public ProductUserBehaviorAcc merge(ProductUserBehaviorAcc a, ProductUserBehaviorAcc b) {
        ProductUserBehaviorAcc acc = new ProductUserBehaviorAcc();

        acc.click = a.click + b.click;
        acc.view = a.view + b.view;
        acc.cart = a.cart + b.cart;

        return acc;
    }
}