package realtime.dws.bean.user;

import com.fasterxml.jackson.annotation.JsonProperty;

public class UserBehavior {
    private Long userId;
    private Long productId;
    @JsonProperty("window_start")
    private Long windowStart;
    private Long windowEnd;
    private Long countCart;
    private Long countClick;
    private Long countView;

    public UserBehavior(){}

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

    public Long getUserId() {
        return userId;
    }

    public Long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(Long windowEnd) {
        this.windowEnd = windowEnd;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    @Override
    public String toString() {
        return "UserBehavior:{" +
                "windowStart=" +windowStart +
                ", windowEnd=" + windowEnd +
                ", userId=" +userId +
                ", productId=" + productId +
                ", countView=" + countView +
                ", countClick=" + countClick +
                ", countCart=" + countCart +
                "}";
    }
}
