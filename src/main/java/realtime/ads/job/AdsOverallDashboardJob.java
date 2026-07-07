package realtime.ads.job;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import realtime.ads.bean.AdsOverallDashboardStats;
import realtime.common.util.KafkaSourceUtil;
import realtime.common.util.sink.KafkaSinkUtil;
import realtime.dws.bean.user.UserFunnelStats;
import realtime.dws.bean.total.TradeStatsTotal;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;

/**
 * ADS 层：运营总览大屏
 *
 * 输入流1：dws_order_trade（整体 GMV 统计 → TradeStatsTotal）
 * 输入流2：dws_user_funnel（用户转化漏斗 → UserFunnelStats）
 * 处理：按 windowStart 双流 Join → 计算全平台核心指标
 * 输出：ads_overall_dashboard
 *
 * 大屏指标：GMV、退款率、客单价、新用户占比、注册→支付转化率
 */
public class AdsOverallDashboardJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        // ==================== 1. 读取双流 ====================

        // GMV 统计流
        DataStream<TradeStatsTotal> gmvSource = KafkaSourceUtil.createKafkaSourceStream(
                env,
                "dws_order_trade",
                "flink-group-ads-dashboard-gmv",
                TradeStatsTotal.class
        );

        // 用户漏斗流
        DataStream<UserFunnelStats> funnelSource = KafkaSourceUtil.createKafkaSourceStream(
                env,
                "dws_user_funnel",
                "flink-group-ads-dashboard-funnel",
                UserFunnelStats.class
        );

        // ==================== 2. 分配水位线 ====================

        SingleOutputStreamOperator<TradeStatsTotal> gmvStream = gmvSource
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<TradeStatsTotal>forBoundedOutOfOrderness(Duration.ofMinutes(2))
                                .withTimestampAssigner((t, ts) -> t.getWindowStart())
                                .withIdleness(Duration.ofSeconds(30))
                );

        SingleOutputStreamOperator<UserFunnelStats> funnelStream = funnelSource
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<UserFunnelStats>forBoundedOutOfOrderness(Duration.ofMinutes(2))
                                .withTimestampAssigner((f, ts) -> f.getWindowStart())
                                .withIdleness(Duration.ofSeconds(30))
                );

        // ==================== 3. 按窗口起始时间 keyBy ====================

        KeyedStream<TradeStatsTotal, Long> gmvKeyed = gmvStream.keyBy(TradeStatsTotal::getWindowStart);
        KeyedStream<UserFunnelStats, Long> funnelKeyed = funnelStream.keyBy(UserFunnelStats::getWindowStart);

        // ==================== 4. 双流 Join + 派生指标计算 ====================

        DataStream<AdsOverallDashboardStats> dashboardStream = gmvKeyed
                .connect(funnelKeyed)
                .process(new DashboardJoinFunction());

        // ==================== 5. 输出 ====================

        dashboardStream.print("运营总览:");
         KafkaSink<AdsOverallDashboardStats> sink = KafkaSinkUtil.createSink(
                 "ads_overall_dashboard",
                 AdsOverallDashboardStats.class,
                 AdsOverallDashboardStats::getWindowStart,
                 "ads_overall_dashboard_tx_"
         );
         dashboardStream.sinkTo(sink);

        env.execute("AdsOverallDashboardJob");
    }

    // ========================================================================
    // 双流 Join ProcessFunction（非抽象类）
    // ========================================================================
    public static class DashboardJoinFunction
            extends KeyedCoProcessFunction<Long, TradeStatsTotal, UserFunnelStats, AdsOverallDashboardStats> {

        // 只缓存漏斗流数据（因为 GMV 流每 10 秒都有数据，漏斗流可能延迟到达）
        private ValueState<UserFunnelStats> funnelState;

        @Override
        public void open(Configuration parameters) {
            funnelState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("funnelState", UserFunnelStats.class));
        }

        /**
         * GMV 流先到（GMV 每 10s 都有数据，所以以此为主输出）
         * 如果有漏斗数据则合并，没有则漏斗字段用 0
         */
        @Override
        public void processElement1(TradeStatsTotal gmv,
                                    Context ctx,
                                    Collector<AdsOverallDashboardStats> out) throws Exception {
            UserFunnelStats funnel = funnelState.value();
            // 无论漏斗是否存在，都输出（漏斗字段为默认值 0 或实际值）
            out.collect(merge(gmv, funnel));
            // 清除缓存的状态（防止状态无限增长）
            funnelState.clear();
        }

        /**
         * 漏斗流先到（缓存漏斗数据，等待 GMV 流触发输出）
         */
        @Override
        public void processElement2(UserFunnelStats funnel,
                                    Context ctx,
                                    Collector<AdsOverallDashboardStats> out) throws Exception {
            // 更新漏斗状态（缓存）
            funnelState.update(funnel);
            // 注意：不在这里输出，等待 GMV 流触发 processElement1
        }

        /**
         * 合并双流数据，计算派生指标
         * funnel 可能为 null（漏斗流还没到），此时漏斗字段默认 0
         */
        private static AdsOverallDashboardStats merge(TradeStatsTotal gmv, UserFunnelStats funnel) {

            long orderCount   = getOrDefault(gmv.getOrderCount());
            long payUserCount = getOrDefault(gmv.getPayUserCount());
            long newUserCount = getOrDefault(gmv.getNewUserCount());
            long oldUserCount = getOrDefault(gmv.getOldUserCount());
            long refundAmt    = getOrDefault(gmv.getRefundAmount());
            long totalGmv     = getOrDefault(gmv.getGmv());
            Long registerCount = funnel == null ? null : funnel.getRegisterCount();
            Long convertedCount = funnel == null ? null : funnel.getConvertedCount();

            long totalPayAmount = totalGmv + refundAmt;   // 含退款的总支付金额

            AdsOverallDashboardStats result = new AdsOverallDashboardStats();
            result.setWindowStart(gmv.getWindowStart());
            result.setWindowEnd(gmv.getWindowEnd());

            // GMV 指标
            result.setGmv(totalGmv);
            result.setRefundAmount(refundAmt);
            result.setOrderCount(orderCount);
            result.setPayUserCount(payUserCount);
            result.setNewUserCount(newUserCount);
            result.setOldUserCount(oldUserCount);

            // 漏斗指标
            result.setConvertedCount(convertedCount);
            result.setRegisterCount(
                    funnel == null ? null : getOrDefault(funnel.getRegisterCount())
            );

            // 退款率 = refund / (gmv + refund)
            result.setRefundRate(
                    totalPayAmount == 0 ? BigDecimal.ZERO :
                            BigDecimal.valueOf(refundAmt)
                                    .divide(BigDecimal.valueOf(totalPayAmount), 4, RoundingMode.HALF_UP)
            );

            // 客单价 = (gmv + refund) / orderCount
            result.setAov(
                    orderCount == 0 ? BigDecimal.ZERO :
                            BigDecimal.valueOf(totalPayAmount)
                                    .divide(BigDecimal.valueOf(orderCount), 2, RoundingMode.HALF_UP)
            );

            // 新用户占比 = newUserCount / payUserCount
            result.setNewUserRatio(
                    payUserCount == 0 ? BigDecimal.ZERO :
                            BigDecimal.valueOf(newUserCount)
                                    .divide(BigDecimal.valueOf(payUserCount), 4, RoundingMode.HALF_UP)
            );

            // 注册→支付转化率 = convertedCount / registerCount
            result.setRegisterToPayRate(
                    registerCount == null || registerCount == 0 ? null :
                            BigDecimal.valueOf(convertedCount)
                                    .divide(BigDecimal.valueOf(registerCount), 4, RoundingMode.HALF_UP)
            );

            return result;
        }

        private static long getOrDefault(Long value) {
            return value == null ? 0L : value;
        }
    }
}