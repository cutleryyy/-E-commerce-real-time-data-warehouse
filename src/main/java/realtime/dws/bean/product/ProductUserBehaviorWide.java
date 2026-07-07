package realtime.dws.bean.product;

import java.math.BigDecimal;

public class ProductUserBehaviorWide extends TradeProductStatsWide {
    private Long countCart;
    private Long countClick;
    private Long countView;

    public ProductUserBehaviorWide(){}

    public Long getCountView() {
        return countView;
    }

    public void setCountView(Long countView) {
        this.countView = countView;
    }

    public Long getCountClick() {
        return countClick;
    }

    public void setCountClick(Long countClick) {
        this.countClick = countClick;
    }

    public Long getCountCart() {
        return countCart;
    }

    public void setCountCart(Long countCart) {
        this.countCart = countCart;
    }

    @Override
    public String getCategoryName() {
        return super.getCategoryName();
    }

    @Override
    public void setProductName(String productName) {
        super.setProductName(productName);
    }

    @Override
    public void setPrice(BigDecimal price) {
        super.setPrice(price);
    }

    @Override
    public void setCategoryName(String categoryName) {
        super.setCategoryName(categoryName);
    }

    @Override
    public void setCategoryId(Long categoryId) {
        super.setCategoryId(categoryId);
    }

    @Override
    public String getProductName() {
        return super.getProductName();
    }

    @Override
    public BigDecimal getPrice() {
        return super.getPrice();
    }

    @Override
    public Long getCategoryId() {
        return super.getCategoryId();
    }

    @Override
    public void setWindowEnd(Long windowEnd) {
        super.setWindowEnd(windowEnd);
    }

    @Override
    public Long getWindowStart() {
        return super.getWindowStart();
    }

    @Override
    public void setWindowStart(Long windowStart) {
        super.setWindowStart(windowStart);
    }

    @Override
    public Long getWindowEnd() {
        return super.getWindowEnd();
    }

    @Override
    public String toString() {
        return "TradeStats:{" +
                "windowStart=" + getWindowStart() +
                ", windowEnd=" + getWindowEnd() +
                ", category_id=" + getCategoryId() +
                ", category_name=" + getCategoryName() +
                ", product_id=" + getProductId()+
                ", product_name=" + getProductName() +
                ", product_price=" + getPrice() +
                ", countView=" +countView +
                ", countClick=" +countClick +
                ", countCart" + countCart +
                "}";
    }
}
