package realtime.dws.bean.product;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ProductTradeStats {
    private Long productId;
    @JsonProperty("window_start")
    private Long windowStart;
    private Long windowEnd;
    private Long productGmv;
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

    public Long getProductGmv() {
        return productGmv;
    }

    public void setProductGmv(Long productGmv) {
        this.productGmv = productGmv;
    }

    public Long getProductId() {
        return productId;
    }

    public void setProductId(Long productId) {
        this.productId = productId;
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
        return "TradeStats:{" +
                "windowStart=" + windowStart +
                ", windowEnd=" +windowEnd +
                ", productId=" + productId +
                ", gmv=" + productGmv +
                ", refundAmount=" + refundAmount +
                ", orderCount=" + orderCount +
                ", payUserCount" +payUserCount +
                ", newUserCount=" +  newUserCount +
                ", oldUserCount=" + oldUserCount +
                "}";
    }
}