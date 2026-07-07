package realtime.ads.bean;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.math.BigDecimal;

public class AdsProductBehaviorBean {

    @JsonProperty("product_id")
    private Long productId;

    @JsonProperty("product_name")
    private String productName;

    @JsonProperty("category_id")
    private Long categoryId;

    @JsonProperty("category_name")
    protected String categoryName;

    @JsonProperty("price")
    private BigDecimal price;

    @JsonProperty("count_cart")
    private Long countCart;

    @JsonProperty("count_click")
    private Long countClick;

    @JsonProperty("count_view")
    private Long countView;

    @JsonProperty("window_start")
    private Long windowStart;

    @JsonProperty("window_end")
    private Long windowEnd;
    public AdsProductBehaviorBean(){}

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

    public void setCategoryId(Long categoryId) {
        this.categoryId = categoryId;
    }

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
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
//        return "TradeStats:{" +
//                "window_start=" + windowStart +
//                ", window_end=" + windowEnd +
//                ", category_id=" + categoryId +
//                ", category_name=" + categoryName +
//                ", product_id=" + productId+
//                ", product_name=" + productName +
//                ", product_price=" + price +
//                ", count_view=" +countView +
//                ", count_click=" +countClick +
//                ", count_cart" + countCart +
//                "}";
//    }
}
