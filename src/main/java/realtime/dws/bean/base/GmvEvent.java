package realtime.dws.bean.base;

import com.fasterxml.jackson.annotation.JsonProperty;

public class GmvEvent {
    @JsonProperty("order_id")
    private Long orderId;
    private Long amountDelta;
    @JsonProperty("timestamp")
    private Long eventTime;
    @JsonProperty("user_id")
    private Long userId;
    @JsonProperty("is_new_user")
    private Integer isNewUser;
    @JsonProperty("category_id")
    private Long categoryId;
    private String channel;
    @JsonProperty("product_id")
    private Long productId;
    private String province;
    public GmvEvent(){};

    public GmvEvent(Long orderId,
                    Long userId,
                    Long categoryId,
                    Long productId,
                    Long amountDelta,
                    String channel,
                    Integer isNewUser,
                    Long eventTime,
                    String province
                    ) {

        this.orderId = orderId;
        this.userId = userId;
        this.categoryId = categoryId;
        this.productId = productId;
        this.amountDelta = amountDelta;
        this.channel = channel;
        this.isNewUser = isNewUser;
        this.eventTime = eventTime;
        this.province = province;
    }

//    public GmvEvent(Long orderId, Integer categoryId, int productId, Long amount, String channel, Integer isNewUser, Long eventTime, String province, Integer isNewUser1) {
//    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getChannel() {
        return channel;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public Long getProductId() {
        return productId;
    }

    public void setProductId(Long productId) {
        this.productId = productId;
    }

    public Long getCategoryId() {
        return categoryId;
    }

    public void setCategoryId(Long categoryId) {
        this.categoryId = categoryId;
    }

    public Integer getIsNewUser() {
        return isNewUser;
    }

    public void setIsNewUser(Integer isNewUser) {
        this.isNewUser = isNewUser;
    }


    public Long getEventTime() {
        return eventTime;
    }

    public void setEventTime(Long eventTime) {
        this.eventTime = eventTime;
    }

    public Long getAmountDelta() {
        return amountDelta;
    }

    public void setAmountDelta(Long amountDelta) {
        this.amountDelta = amountDelta;
    }

    public Long getOrderId() {
        return orderId;
    }

    public void setOrderId(Long orderId) {
        this.orderId = orderId;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    @Override
    public String toString() {
        return "{GmvEvent:"+
                " order_id=" + orderId +
                ", user_id=" + userId+
                ", category_id=" + categoryId +
                ", product_id=" + productId +
                ", province='" + province +
                ", channel=" + channel +
                ", is_new_user=" + isNewUser+
                ", amountDelta =" + amountDelta +
                "}";
    }

}
