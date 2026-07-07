package realtime.ads.bean;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.math.BigDecimal;

/**
 * ADS 层：商品实时排行输出 Bean
 * 供 Doris 表直接消费，Doris 端通过 ORDER BY gmv DESC LIMIT N 做排序
 */
public class AdsProductRankStats {

    @JsonProperty("window_start")
    private Long windowStart;

    @JsonProperty("window_end")
    private Long windowEnd;

    @JsonProperty("product_id")
    private Long productId;

    @JsonProperty("product_name")
    private String productName;

    @JsonProperty("category_id")
    private Long categoryId;

    @JsonProperty("category_name")
    private String categoryName;

    @JsonProperty("price")
    private BigDecimal price;

    // 汇总指标
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

    // 派生指标
    @JsonProperty("refund_rate")
    private BigDecimal refundRate;

    @JsonProperty("aov")
    private BigDecimal aov;

    @JsonProperty("new_user_ratio")
    private BigDecimal newUserRatio;

    public AdsProductRankStats(){}

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public Long getCategoryId() {
        return categoryId;
    }

    public void setCategoryId(Long categoryId) {
        this.categoryId = categoryId;
    }

    public String getCategoryName() {
        return categoryName;
    }

    public void setCategoryName(String categoryName) {
        this.categoryName = categoryName;
    }

    public BigDecimal getPrice() {
        return price;
    }

    public void setPrice(BigDecimal price) {
        this.price = price;
    }

    public Long getRefundAmount() {
        return refundAmount;
    }

    public void setRefundAmount(Long refundAmount) {
        this.refundAmount = refundAmount;
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

    public BigDecimal getNewUserRatio() {
        return newUserRatio;
    }

    public void setNewUserRatio(BigDecimal newUserRatio) {
        this.newUserRatio = newUserRatio;
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

    public Long getProductId() {
        return productId;
    }

    public void setProductId(Long productId) {
        this.productId = productId;
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
//        return "AdsProductRanking:{" +
//                "window_start=" + windowStart +
//                ", window_end=" + windowEnd +
//                ", product_id=" + productId +
//                ", product_name=" + productName +
//                ", category_id=" + categoryId +
//                ", category_name=" + categoryName +
//                ", price=" + price +
//                ", gmv=" + gmv +
//                ", refund_amount=" + refundAmount +
//                ", order_count=" + orderCount +
//                ", pay_user_count=" + payUserCount +
//                ", new_user_count=" + newUserCount +
//                ", old_user_count=" + oldUserCount +
//                ", refund+rate=" + refundRate +
//                ", aov=" + aov +
//                ", new_user_ratio=" + newUserRatio +
//                "}";
//    }
}
