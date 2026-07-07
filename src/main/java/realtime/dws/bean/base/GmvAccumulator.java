package realtime.dws.bean.base;

import com.fasterxml.jackson.annotation.JsonProperty;

public class GmvAccumulator {
    private  Long gmv = 0L;
    private Long refundAmount = 0L;
    public Long orderCount;
    @JsonProperty("window_start")
    private Long windowStart;
    private Long windowEnd;
    private Long payUserCount;
    private Long newUserCount;
    private Long oldUserCount;
    public GmvAccumulator(){}

    public Long getWindowStart() {
        return windowStart;
    }

    public void setWindowStart(Long windowStart) {
        this.windowStart = windowStart;
    }

    public Long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(Long windowEnd) {
        this.windowEnd = windowEnd;
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

    public long getOrderCount() {
        return orderCount;
    }

    public void setOrderCount(long orderCount) {
        this.orderCount = orderCount;
    }

    public Long getRefundAmount() {
        return refundAmount;
    }
    public void setRefundAmount(Long refundAmount) {
        this.refundAmount = refundAmount;
    }
    public void setGmv(Long gmv) {
        this.gmv = gmv;
    }
    public void add(Long amount){
        gmv +=amount;

    }
    public Long getGmv() {
        return gmv;
    }
}
