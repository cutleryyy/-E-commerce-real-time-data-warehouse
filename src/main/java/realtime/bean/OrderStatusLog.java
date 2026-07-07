package realtime.bean;

import com.fasterxml.jackson.annotation.JsonProperty;

public class OrderStatusLog extends BaseEvent{
    @JsonProperty("order_id")
    private Long orderId;
    @JsonProperty("product_id")
    private Long productId;
    private String status;
    private Long amount;
    @JsonProperty("timestamp")
    private Long eventTime;
    @JsonProperty("category_id")
    private Long categoryId;
    private String channel;
    private Integer is_new_user;
    private Long etlTime;

    public OrderStatusLog(){}

    public Long getEtlTime() {
        return etlTime;
    }

    public void setEtlTime(Long etlTime) {
        this.etlTime = etlTime;
    }

    public Integer getIs_new_user() {
        return is_new_user;
    }

    public void setIs_new_user(Integer is_new_user) {
        this.is_new_user = is_new_user;
    }

    public Long getOrderId() {
        return orderId;
    }

    public void setOrderId(Long orderId) {
        this.orderId = orderId;
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

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Long getAmount() {
        return amount;
    }

    public void setAmount(Long amount) {
        this.amount = amount;
    }

    @Override
    public Long getUserId() {
        return super.getUserId();
    }

    @Override
    public void setUserId(Long userId) {
        super.setUserId(userId);
    }

    @Override
    public Long getEventTime() {
        return eventTime;
    }

    @Override
    public void setEventTime(Long eventTime) {
        this.eventTime = eventTime;
    }

    public Long getCategoryId() {
        return categoryId;
    }

    public String getChannel() {
        return channel;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    @Override
    public String toString() {
        return "OrderStatusLog{" +
                "user_id=" + userId +
                ", order_id=" + orderId +
                ", product_id=" +productId+
                ", category_id="+categoryId +
                ", status=" + status+
                ", amount=" + amount +
                ", province=" + province +
                ", is_new_user=" + is_new_user +
                ", channel=" + channel +
                ", eventTime=" + eventTime +
                '}';
    }
}
