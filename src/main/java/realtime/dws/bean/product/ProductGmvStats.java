package realtime.dws.bean.product;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ProductGmvStats {
    private Long productId;
    @JsonProperty("window_start")
    private Long windowStart;
    private Long windowEnd;
    private Long productGmv;
    private Long refundAmount;

    public ProductGmvStats(){}


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
        return "ProductTradeStats:{" +
                "windowStart=" + windowStart +
                ", windowEnd=" +windowEnd +
                ", productId=" + productId +
                ", productGmv=" +productGmv +
                ", refundAmount=" + refundAmount +
                "}";
    }
}