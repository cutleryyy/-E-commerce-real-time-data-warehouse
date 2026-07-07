package realtime.dws.bean.total;

import com.fasterxml.jackson.annotation.JsonProperty;

public class GmvStats {
    @JsonProperty("window_start")
    private Long windowStart;
    private Long windowEnd;
    private Long gmv;
    private Long orderCount;
    private Long refundAmount;
    private Long newUserCount;
    private Long oldUserCount;
    public GmvStats(){};

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
    public Long getRefundAmount() {
        return refundAmount;
    }

    public void setRefundAmount(Long refundAmount) {
        this.refundAmount = refundAmount;
    }

    public Long getOrderCount() {
        return orderCount;
    }

    public void setOrderCount(Long orderCount) {
        this.orderCount = orderCount;
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

    @Override
    public String toString() {
        return "{GmvStats:windowStart" + windowStart +
                ", windowEnd" + windowEnd +
                ", gmv" + gmv  +
                ", refundAmount" + refundAmount +
                ", orderCount" + orderCount +
                ", newUserCount" + newUserCount +
                ", oldUserCount" + oldUserCount +
                "}";
    }
}
