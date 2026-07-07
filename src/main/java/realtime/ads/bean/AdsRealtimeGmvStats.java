package realtime.ads.bean;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

public class AdsRealtimeGmvStats {
    @JsonProperty("window_start")
    private Long windowStart;

    @JsonProperty("window_end")
    private Long windowEnd;

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

    public AdsRealtimeGmvStats(){}

    public Long getOldUserCount() {
        return oldUserCount;
    }

    public void setOldUserCount(Long oldUserCount) {
        this.oldUserCount = oldUserCount;
    }

    public Long getNewUserCount() {
        return newUserCount;
    }

    public void setNewUserCount(Long newUserCount) {
        this.newUserCount = newUserCount;
    }

    public Long getPayUserCount() {
        return payUserCount;
    }

    public void setPayUserCount(Long payUserCount) {
        this.payUserCount = payUserCount;
    }

    public Long getOrderCount() {
        return orderCount;
    }

    public void setOrderCount(Long orderCount) {
        this.orderCount = orderCount;
    }

    public Long getRefundAmount() {
        return refundAmount;
    }

    public void setRefundAmount(Long refundAmount) {
        this.refundAmount = refundAmount;
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

    private static final ObjectMapper mapper = new ObjectMapper();
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
//                "window_start" + windowStart +
//                ",window_end=" + windowEnd +
//                ",gmv=" + gmv +
//                ",refund_amount=" + refundAmount +
//                ",order_count=" + orderCount +
//                ",pay_user_count=" + payUserCount +
//                ",new_user_count=" + newUserCount +
//                ",old_user_count=" + oldUserCount +
//                "}";
//    }
}
