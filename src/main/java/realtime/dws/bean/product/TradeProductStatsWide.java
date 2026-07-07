package realtime.dws.bean.product;

import java.math.BigDecimal;

public class TradeProductStatsWide extends ProductTradeStats {
    private String productName;
    private Long categoryId;
    protected String categoryName;
    private BigDecimal price;
    public TradeProductStatsWide(){}

    public BigDecimal getPrice() {
        return price;
    }

    public void setPrice(BigDecimal price) {
        this.price = price;
    }

    public String getCategoryName() {
        return categoryName;
    }

    public void setCategoryName(String categoryName) {
        this.categoryName = categoryName;
    }

    public Long getCategoryId() {
        return categoryId;
    }

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    @Override
    public Long getPayUserCount() {
        return super.getPayUserCount();
    }

    @Override
    public Long getProductId() {
        return super.getProductId();
    }

    @Override
    public void setProductId(Long productId) {
        super.setProductId(productId);
    }

    @Override
    public Long getNewUserCount() {
        return super.getNewUserCount();
    }

    @Override
    public Long getOldUserCount() {
        return super.getOldUserCount();
    }

    @Override
    public void setPayUserCount(Long payUserCount) {
        super.setPayUserCount(payUserCount);
    }

    @Override
    public Long getOrderCount() {
        return super.getOrderCount();
    }

    @Override
    public Long getRefundAmount() {
        return super.getRefundAmount();
    }

    @Override
    public Long getWindowEnd() {
        return super.getWindowEnd();
    }
    @Override
    public void setOldUserCount(Long oldUserCount) {
        super.setOldUserCount(oldUserCount);
    }

    @Override
    public Long getWindowStart() {
        return super.getWindowStart();
    }

    @Override
    public void setOrderCount(Long orderCount) {
        super.setOrderCount(orderCount);
    }

    @Override
    public void setRefundAmount(Long refundAmount) {
        super.setRefundAmount(refundAmount);
    }

    public void setCategoryId(Long categoryId) {
        this.categoryId = categoryId;
    }

    @Override
    public Long getProductGmv() {
        return super.getProductGmv();
    }

    @Override
    public void setProductGmv(Long productGmv) {
        super.setProductGmv(productGmv);
    }

    @Override
    public void setNewUserCount(Long newUserCount) {
        super.setNewUserCount(newUserCount);
    }

    @Override
    public void setWindowEnd(Long windowEnd) {
        super.setWindowEnd(windowEnd);
    }

    @Override
    public void setWindowStart(Long windowStart) {
        super.setWindowStart(windowStart);
    }

    @Override
    public String toString() {
        return "TradeStats:{" +
                "windowStart=" + getWindowStart() +
                ", windowEnd=" + getWindowEnd() +
                ", category_id=" + categoryId +
                ", category_name=" + categoryName +
                ", product_id=" + getProductId()+
                ", product_name=" + productName +
                ", product_price=" + price +
                ", gmv=" + getProductGmv() +
                ", refundAmount=" + getRefundAmount() +
                ", orderCount=" + getOrderCount() +
                ", payUserCount" +getPayUserCount()+
                ", newUserCount=" +  getNewUserCount() +
                ", oldUserCount=" + getOldUserCount() +
                "}";
    }
}
