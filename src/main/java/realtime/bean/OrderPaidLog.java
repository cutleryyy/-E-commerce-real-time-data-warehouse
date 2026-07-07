package realtime.bean;

import com.fasterxml.jackson.annotation.JsonProperty;

public class OrderPaidLog extends BaseEvent{
    @JsonProperty("order_id")
    private Long orderId;
    @JsonProperty("product_id")
    private Long productId;
    @JsonProperty("category_id")
    private Long categoryId;
    private String channel;
    @JsonProperty("is_new_user")
    private int is_New_user;
    private Long amount;
    public OrderPaidLog(){}

    @Override
    public Long getUserId() {
        return super.getUserId();
    }

    @Override
    public String getProvince() {
        return super.getProvince();
    }

    @Override
    public Long getEventTime() {
        return super.getEventTime();
    }

    @Override
    public void setProvince(String province) {
        super.setProvince(province);
    }

    @Override
    public void setEventTime(Long eventTime) {
        super.setEventTime(eventTime);
    }

    @Override
    public void setUserId(Long userId) {
        super.setUserId(userId);
    }

    public Long getOrderId() {
        return orderId;
    }

    public void setOrderId(Long orderId) {
        this.orderId = orderId;
    }

    public String getChannel() {
        return channel;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public int getIs_New_user() {
        return is_New_user;
    }

    public void setIs_New_user(int is_New_user) {
        this.is_New_user = is_New_user;
    }

    public long getAmount() {
        return amount;
    }

    public void setAmount(long amount) {
        this.amount = amount;
    }

    public void setAmount(Long amount) {
        this.amount = amount;
    }

    public Long getCategoryId() {
        return categoryId;
    }

    public void setCategoryId(Long categoryId) {
        this.categoryId = categoryId;
    }

    public Long getProductId() {
        return productId;
    }

    public void setProductId(Long productId) {
        this.productId = productId;
    }

    @Override
    public String toString() {
        return "OrderPaidLog{" +
                "userId=" + getUserId() +
                ", orderId=" + orderId +
                ", category_id=" + categoryId +
                ", amount=" + amount +
                ", channel=" + channel +
                ", is_new_user=" + is_New_user +
                ", eventTime=" + getEventTime() +
                ", province=" + getProvince() +
                '}';
    }
}