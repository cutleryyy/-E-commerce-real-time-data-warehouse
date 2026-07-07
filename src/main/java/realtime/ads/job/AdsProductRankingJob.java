package realtime.ads.job;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import realtime.ads.aggregate.ProductRankingAgg;
import realtime.ads.bean.AdsProductRankStats;
import realtime.ads.process.ProductRankingProcess;
import realtime.common.util.FlinkEnvCheckpointUtil;
import realtime.common.util.KafkaSourceUtil;
import realtime.common.util.sink.KafkaSinkUtil;
import realtime.dws.bean.product.TradeProductStatsWide;

import java.time.Duration;
/**
 * ADS 层：商品实时排行榜
 *
 * 输入：dws_product_trade_wide（DWS 层商品交易宽表，10s 粒度）
 * 处理：1min 窗口二次聚合 → 计算派生指标
 * 输出：ads_product_ranking（Doris 直接消费，在 SQL 层做 ORDER BY gmv DESC）
 */
public class AdsProductRankingJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        FlinkEnvCheckpointUtil.enableCheckpoint(env);
        DataStream<TradeProductStatsWide> source = KafkaSourceUtil.createKafkaSourceStream(
                env,
                "dws_product_trade_wide",
                "flink-group-ads-productRanking",
                TradeProductStatsWide.class
        );
        // 分配水位线
        SingleOutputStreamOperator<TradeProductStatsWide> adsProductRankStream = source
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<TradeProductStatsWide>forBoundedOutOfOrderness(Duration.ofMinutes(2))
                                .withTimestampAssigner((tradeProductStatsWide, ts)
                                        -> tradeProductStatsWide.getWindowStart())
                                .withIdleness(Duration.ofSeconds(30))
                );
        // 按商品id分组
        KeyedStream<TradeProductStatsWide, Long> keyedStream = adsProductRankStream
                .keyBy(TradeProductStatsWide::getProductId);

        DataStream<AdsProductRankStats> rankingStream = keyedStream
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .allowedLateness(Time.minutes(5))
                // 计算退款率、客单价、新用户占比
                .aggregate(new ProductRankingAgg(),new ProductRankingProcess());
        
//        输出到 Kafka（Doris 通过 Routine Load 消费）
        KafkaSink<AdsProductRankStats> sink = KafkaSinkUtil.createSink(
                "ads_product_ranking",
                AdsProductRankStats.class,
                AdsProductRankStats::getProductId,
                "ads_product_ranking_tx_"
        );
        rankingStream.sinkTo(sink);
        rankingStream.print("商品排行:");

        env.execute("AdsProductRankingJob");
    }
}
