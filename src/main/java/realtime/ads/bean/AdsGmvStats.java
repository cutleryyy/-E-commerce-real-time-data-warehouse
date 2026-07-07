package realtime.ads.bean;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;

import java.math.BigDecimal;

public class AdsGmvStats {
    private Long windowStart;

    private Long windowEnd;

    private Long refundAmount;

    Long gmv;

    Long orderCount;
//    @JsonProperty("pay_user_count")
    Long payUserCount;
//    @JsonProperty("new_user_count")
    Long newUserCount;
//    @JsonProperty("old_user_count")
    Long oldUserCount;

// conversion_rate = payUserCount / newUserCount
//    @JsonProperty("new_user_ratio")
    BigDecimal newUserRatio;
    // refundAmount / gmv
//    @JsonProperty("refund_rate")
    BigDecimal refundRate;
    // gmv / payUserCount
    BigDecimal aov;
//    @JsonProperty("register_count")
    private Long registerCount;
    //    @JsonProperty("converted_count")
    private Long convertedCount;
    public AdsGmvStats(){}

    public BigDecimal getNewUserRatio() {
        return newUserRatio;
    }

    public void setNewUserRatio(BigDecimal newUserRatio) {
        this.newUserRatio = newUserRatio;
    }

    public Long getRegisterCount() { return registerCount; }
    public void setRegisterCount(Long registerCount) { this.registerCount = registerCount; }
    public Long getConvertedCount() { return convertedCount; }
    public void setConvertedCount(Long convertedCount) { this.convertedCount = convertedCount; }

    public Long getOrderCount() {
        return orderCount;
    }

    public void setOrderCount(Long orderCount) {
        this.orderCount = orderCount;
    }

    public Long getPayUserCount() {
        return payUserCount;
    }

    public void setPayUserCount(Long payUserCount) {
        this.payUserCount = payUserCount;
    }

    public Long getNewUserCount() {
        return newUserCount;
    }

    public void setNewUserCount(Long newUserCount) {
        this.newUserCount = newUserCount;
    }

    public Long getOldUserCount() {
        return oldUserCount;
    }

    public void setOldUserCount(Long oldUserCount) {
        this.oldUserCount = oldUserCount;
    }

    public Long getRefundAmount() {
        return refundAmount;
    }

    public void setRefundAmount(Long refundAmount) {
        this.refundAmount = refundAmount;
    }

    public BigDecimal getRefundRate() {
        return refundRate;
    }

    public void setRefundRate(BigDecimal refundRate) {
        this.refundRate = refundRate;
    }

    public BigDecimal getAov() {
        return aov;
    }

    public void setAov(BigDecimal aov) {
        this.aov = aov;
    }

    public Long getGmv() {
        return gmv;
    }

    public void setGmv(Long gmv) {
        this.gmv = gmv;
    }

    public Long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(Long windowEnd) {
        this.windowEnd = windowEnd;
    }

    public Long getWindowStart() {
        return windowStart;
    }

    public void setWindowStart(Long windowStart) {
        this.windowStart = windowStart;
    }

    private static final ObjectMapper mapper = new ObjectMapper()
            .findAndRegisterModules()
            .setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    public String toJson() {
        try {
            return mapper.writeValueAsString(this);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
//    @Override
//    public String toString() {
//        return "RealtimeGmv:{" +
//                "window_start=" + windowStart +
//                ", window_end=" + windowEnd +
//                ", gmv=" + gmv +
//                ", refund_amount=" + refundAmount +
//                ", order_count=" + orderCount +
//                ", pay_user_count=" + payUserCount +
//                ", new_user_count=" + newUserCount +
//                ", old_user_count=" + oldUserCount +
//                ", conversion_rate=" +newUserRatio +
//                ", refund_rate=" +refundRate +
//                ", aov= " + aov +
//                "}";
//    }
}
