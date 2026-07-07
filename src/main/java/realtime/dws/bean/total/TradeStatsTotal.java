package realtime.dws.bean.total;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

public class TradeStatsTotal {

    public TradeStatsTotal(){}
    @JsonProperty("window_start")
    private Long windowStart;
    private Long windowEnd;
    private Long gmv;
    private Long refundAmount;
    private Long payUserCount;
    private Long orderCount;
    private Long newUserCount;
    private Long oldUserCount;

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
//        return "TradeStats:{" +
//                "windowStart=" + windowStart +
//                ", windowEnd=" +windowEnd +
//                ", gmv=" + gmv +
//                ", refundAmount=" + refundAmount +
//                ", orderCount=" + orderCount +
//                ", payUserCount" +payUserCount +
//                ", newUserCount=" +  newUserCount +
//                ", oldUserCount=" + oldUserCount +
//                "}";
//    }
}