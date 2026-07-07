package realtime.ads.process;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import realtime.ads.bean.AdsProductRankStats;
import realtime.dws.bean.product.TradeProductStatsWide;

import java.math.BigDecimal;
import java.math.RoundingMode;

public class ProductRankingProcess extends ProcessWindowFunction<TradeProductStatsWide, AdsProductRankStats, Long, TimeWindow> {
    @Override
    public void process(Long productId,
                        Context context,
                        Iterable<TradeProductStatsWide> elements,
                        Collector<AdsProductRankStats> out) {

        TradeProductStatsWide acc = elements.iterator().next();
        // 先对已有实施指标做判断
        // 对输入用get
        long gmv = acc.getProductGmv() != null ? acc.getProductGmv() : 0L;
        long refundAmount = acc.getRefundAmount() != null ? acc.getRefundAmount() : 0L;
        long orderCount = acc.getOrderCount() != null ? acc.getOrderCount() : 0L;
        long payUserCount = acc.getPayUserCount() != null ? acc.getPayUserCount() : 0L;
        long newUserCount = acc.getNewUserCount() != null ? acc.getNewUserCount() : 0L;
        long oldUserCount = acc.getOldUserCount() != null ? acc.getOldUserCount() : 0L;

        // 总交易额（含退款，作为分母）
        long totalGmv = gmv + refundAmount;
        // 输出用set
        AdsProductRankStats result = new AdsProductRankStats();
        result.setWindowStart(context.window().getStart());
        result.setWindowEnd(context.window().getEnd());
        // 维度信息
        result.setProductId(acc.getProductId());
        result.setProductName(acc.getProductName());
        result.setCategoryId(acc.getCategoryId());
        result.setCategoryName(acc.getCategoryName());
        result.setPrice(acc.getPrice());
        // 汇总指标
        result.setGmv(gmv);
        result.setRefundAmount(refundAmount);
        result.setOrderCount(orderCount);
        result.setPayUserCount(payUserCount);
        result.setNewUserCount(newUserCount);
        result.setOldUserCount(oldUserCount);
        // 派生指标：退款率
        result.setRefundRate(
                totalGmv == 0 ? BigDecimal.ZERO :
                        BigDecimal.valueOf(refundAmount)
                                .divide(BigDecimal.valueOf(totalGmv), 4, RoundingMode.HALF_UP)
        );

// 派生指标：客单价=gmv/订单数
        result.setAov(
                orderCount == 0 ? BigDecimal.ZERO :
                        BigDecimal.valueOf(totalGmv)
                                .divide(BigDecimal.valueOf(orderCount), 2, RoundingMode.HALF_UP)
        );

// 派生指标：新用户占比
        result.setNewUserRatio(
                payUserCount == 0 ? BigDecimal.ZERO :
                        BigDecimal.valueOf(newUserCount)
                                .divide(BigDecimal.valueOf(payUserCount), 4, RoundingMode.HALF_UP)
        );
        out.collect(result);
    }
}
