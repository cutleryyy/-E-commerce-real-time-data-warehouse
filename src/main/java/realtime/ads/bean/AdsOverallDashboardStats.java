package realtime.ads.bean;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.math.BigDecimal;

/**
 * ADS 层：运营总览大屏输出 Bean
 *
 * 合并 GMV 指标 + 用户漏斗指标，供 Doris 大屏直接查询
 */
public class AdsOverallDashboardStats {

    @JsonProperty("window_start")
    private Long windowStart;

    @JsonProperty("window_end")
    private Long windowEnd;

     // ===== GMV 指标（来自 dws_order_trade） =====
    @JsonProperty("gmv")
    private Long gmv;

    @JsonProperty("refund_amount")
    private Long refundAmount;

    @JsonProperty("order_count")
    private Long orderCount;

    @JsonProperty("pay_user_count")
    private Long payUserCount;

    @JsonProperty("new_user_count")
    private Long newUserCount;

    @JsonProperty("old_user_count")
    private Long oldUserCount;

    // ===== 漏斗指标 =====
    @JsonProperty("register_count")
    private Long registerCount;

    @JsonProperty("converted_count")
    private Long convertedCount;

    // ===== 派生指标 =====
    @JsonProperty("refund_rate")
    private BigDecimal refundRate;

    @JsonProperty("aov")
    private BigDecimal aov;

    @JsonProperty("new_user_ratio")
    private BigDecimal newUserRatio;

    @JsonProperty("register_to_pay_rate")
    private BigDecimal registerToPayRate;


    public AdsOverallDashboardStats() {}

    public Long getWindowStart() { return windowStart; }
    public void setWindowStart(Long windowStart) { this.windowStart = windowStart; }

    public Long getWindowEnd() { return windowEnd; }
    public void setWindowEnd(Long windowEnd) { this.windowEnd = windowEnd; }

    public Long getGmv() { return gmv; }
    public void setGmv(Long gmv) { this.gmv = gmv; }

    public Long getRefundAmount() { return refundAmount; }
    public void setRefundAmount(Long refundAmount) { this.refundAmount = refundAmount; }

    public Long getOrderCount() { return orderCount; }
    public void setOrderCount(Long orderCount) { this.orderCount = orderCount; }

    public Long getPayUserCount() { return payUserCount; }
    public void setPayUserCount(Long payUserCount) { this.payUserCount = payUserCount; }

    public Long getNewUserCount() { return newUserCount; }
    public void setNewUserCount(Long newUserCount) { this.newUserCount = newUserCount; }

    public Long getOldUserCount() { return oldUserCount; }
    public void setOldUserCount(Long oldUserCount) { this.oldUserCount = oldUserCount; }

    public Long getRegisterCount() { return registerCount; }
    public void setRegisterCount(Long registerCount) { this.registerCount = registerCount; }

    public Long getConvertedCount() { return convertedCount; }
    public void setConvertedCount(Long convertedCount) { this.convertedCount = convertedCount; }

    public BigDecimal getRefundRate() { return refundRate; }
    public void setRefundRate(BigDecimal refundRate) { this.refundRate = refundRate; }

    public BigDecimal getAov() { return aov; }
    public void setAov(BigDecimal aov) { this.aov = aov; }

    public BigDecimal getNewUserRatio() { return newUserRatio; }
    public void setNewUserRatio(BigDecimal newUserRatio) { this.newUserRatio = newUserRatio; }

    public BigDecimal getRegisterToPayRate() { return registerToPayRate; }
    public void setRegisterToPayRate(BigDecimal registerToPayRate) { this.registerToPayRate = registerToPayRate; }

    private static final ObjectMapper mapper = new ObjectMapper()
            .findAndRegisterModules();

    public String toJson() {
        try {
            return mapper.writeValueAsString(this);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
//    @Override
//    public String toString() {
//        return "AdsOverallDashboard:{" +
//                "window_start=" + windowStart +
//                ", window_end=" + windowEnd +
//                ", gmv=" + gmv +
//                ", refund_amount=" + refundAmount +
//                ", order_count=" + orderCount +
//                ", pay_user_count=" + payUserCount +
//                ", new_user_count=" + newUserCount +
//                ", old_user_count=" + oldUserCount +
//                ", register_count=" + registerCount +
//                ", converted_count=" + convertedCount +
//                ", refund_rate=" + refundRate +
//                ", aov=" + aov +
//                ", newUser_ratio=" + newUserRatio +
//                ", register_to_pay_rate=" + registerToPayRate +
//                "}";
//    }
}
