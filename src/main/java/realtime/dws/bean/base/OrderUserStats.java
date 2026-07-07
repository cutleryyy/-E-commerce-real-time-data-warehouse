package realtime.dws.bean.base;

import com.fasterxml.jackson.annotation.JsonProperty;

public class OrderUserStats {
    @JsonProperty("window_start")
    private Long windowStart;
    private Long windowEnd;
    private Long orderCount;
    private Long newUserCount;
    private Long oldUserCount;
    public OrderUserStats(){}

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
        return "OrderUserCount{" +
                "windowStart" + windowStart +
                ", windowEnd=" + windowEnd +
                ", orderCount=" + orderCount +
                ", newUserCount=" + newUserCount+
                ", oldUserCount=" +oldUserCount+
                "}";
    }
}
