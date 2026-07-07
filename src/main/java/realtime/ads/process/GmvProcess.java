package realtime.ads.process;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import realtime.ads.bean.AdsGmvStats;
import realtime.dws.bean.base.GmvAccumulator;
import java.math.BigDecimal;
import java.math.RoundingMode;

public class GmvProcess extends ProcessWindowFunction<GmvAccumulator, AdsGmvStats, String, TimeWindow> {

    @Override
    public void process(String key,
                        Context context,  // 注意：这里直接用 Context，不是 ProcessWindowFunction.Context
                        Iterable<GmvAccumulator> elements,
                        Collector<AdsGmvStats> out) throws Exception {   // 必须声明 throws Exception

        GmvAccumulator acc = elements.iterator().next();

        AdsGmvStats ads = new AdsGmvStats();

        // 时间窗口
        ads.setWindowStart(context.window().getStart());
        ads.setWindowEnd(context.window().getEnd());

        // 基础指标
        long gmv = acc.getGmv();
        long refundAmount = acc.getRefundAmount();
        long orderCount = acc.getOrderCount();
        long payUserCount = acc.getPayUserCount();
        long newUserCount = acc.getNewUserCount();
        long oldUserCount = acc.getOldUserCount();

        ads.setGmv(gmv);
        ads.setRefundAmount(refundAmount);
        ads.setOrderCount(orderCount);
        ads.setPayUserCount(payUserCount);
        ads.setNewUserCount(newUserCount);
        ads.setOldUserCount(oldUserCount);

        // ===== 派生指标 =====
        // 总支付金额（分母）
        long totalGmv = gmv + refundAmount;  // 总支付金额（含退款）

// 1. 净 GMV
        ads.setGmv(acc.getGmv());

// 2. 退款率 = refund / totalGmv
        ads.setRefundRate(
                totalGmv == 0 ? BigDecimal.ZERO :
                        BigDecimal.valueOf(refundAmount)
                                .divide(BigDecimal.valueOf(totalGmv), 4, RoundingMode.HALF_UP)
        );

// 3. 客单价 = totalGmv / orderCount
        ads.setAov(
                orderCount == 0 ? BigDecimal.ZERO :
                        BigDecimal.valueOf(totalGmv)
                                .divide(BigDecimal.valueOf(orderCount), 2, RoundingMode.HALF_UP)
        );

// 4. 新用户占比 = 新用户支付人数 / 总支付人数
        if (payUserCount == 0) {
            ads.setNewUserRatio(BigDecimal.ZERO);
        } else {
            ads.setNewUserRatio(
                    BigDecimal.valueOf(newUserCount)
                            .divide(BigDecimal.valueOf(payUserCount), 4, RoundingMode.HALF_UP)
            );
        }
// 5. 其他字段直接获取
        ads.setRefundAmount(acc.getRefundAmount());
        ads.setOrderCount(acc.getOrderCount());
        ads.setPayUserCount(acc.getPayUserCount());
        ads.setNewUserCount(acc.getNewUserCount());
        ads.setOldUserCount(acc.getOldUserCount());
        ads.setGmv(acc.getGmv());


        out.collect(ads);
    }
}