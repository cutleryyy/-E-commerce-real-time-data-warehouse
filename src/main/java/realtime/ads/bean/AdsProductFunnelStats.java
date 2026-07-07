package realtime.ads.bean;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.math.BigDecimal;

/**
 * ADS 层：商品转化漏斗
 *
 * 浏览 → 点击 → 加购 → 支付 各环节转化率
 */
public class AdsProductFunnelStats {

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

    // 各环节人数
    @JsonProperty("count_view")
    private Long countView;

    @JsonProperty("count_click")
    private Long countClick;

    @JsonProperty("count_cart")
    private Long countCart;

    @JsonProperty("pay_user_count")
    private Long payUserCount;

    @JsonProperty("gmv")
    private Long gmv;

    // 转化率
    @JsonProperty("view_to_click_rate")   // 点击率 = click / view
    private BigDecimal viewToClickRate;

    @JsonProperty("click_to_cart_rate")   // 加购率 = cart / click
    private BigDecimal clickToCartRate;

    @JsonProperty("cart_to_pay_rate")     // 支付转化率 = pay / cart
    private BigDecimal cartToPayRate;

    @JsonProperty("view_to_pay_rate")     // 整体转化率 = pay / view
    private BigDecimal viewToPayRate;
    public AdsProductFunnelStats(){};

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

    public Long getProductId() {
        return productId;
    }

    public void setProductId(Long productId) {
        this.productId = productId;
    }

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

    public Long getPayUserCount() {
        return payUserCount;
    }

    public void setPayUserCount(Long payUserCount) {
        this.payUserCount = payUserCount;
    }

    public Long getGmv() {
        return gmv;
    }

    public void setGmv(Long gmv) {
        this.gmv = gmv;
    }

    public BigDecimal getViewToClickRate() {
        return viewToClickRate;
    }

    public void setViewToClickRate(BigDecimal viewToClickRate) {
        this.viewToClickRate = viewToClickRate;
    }

    public BigDecimal getClickToCartRate() {
        return clickToCartRate;
    }

    public void setClickToCartRate(BigDecimal clickToCartRate) {
        this.clickToCartRate = clickToCartRate;
    }

    public BigDecimal getCartToPayRate() {
        return cartToPayRate;
    }

    public void setCartToPayRate(BigDecimal cartToPayRate) {
        this.cartToPayRate = cartToPayRate;
    }

    public BigDecimal getViewToPayRate() {
        return viewToPayRate;
    }

    public void setViewToPayRate(BigDecimal viewToPayRate) {
        this.viewToPayRate = viewToPayRate;
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
//        return "AdsProductFunnel:{" +
//                "window_start=" + windowStart +
//                ", window_end=" + windowEnd +
//                ", product_id=" + productId +
//                ", product_name=" + productName +
//                ", category_id=" + categoryId +
//                ", category_name=" + categoryName +
//                ", price=" + price +
//                ", count_view=" + countView +
//                ", count_click=" + countClick +
//                ", count_cart=" + countCart +
//                ", pay_user_count=" + payUserCount +
//                ", gmv=" + gmv +
//                ", view_to_click_rate=" + viewToClickRate +
//                ", click_to_cart_rate=" + clickToCartRate +
//                ", cart_to_pay_rate=" + cartToPayRate +
//                ", view_to_pay_rate=" + viewToPayRate +
//                "}";
//    }
}
