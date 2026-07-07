package realtime.bean;

import com.fasterxml.jackson.annotation.JsonProperty;

public class UserClickLog extends BaseEvent{
    private String action;
    @JsonProperty("product_id")
    private Long productId;
    @JsonProperty("category_id")
    private Long categoryId;
    private String channel;
    public UserClickLog(){}


    @Override
    public Long getEventTime() {
        return super.getEventTime();
    }

    @Override
    public String getProvince() {
        return super.getProvince();
    }

    @Override
    public Long getUserId() {
        return super.getUserId();
    }

    @Override
    public void setEventTime(Long eventTime) {
        super.setEventTime(eventTime);
    }

    @Override
    public void setProvince(String province) {
        super.setProvince(province);
    }

    @Override
    public void setUserId(Long userId) {
        super.setUserId(userId);
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
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

    public String getChannel() {
        return channel;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }
    @Override
    public String toString(){
        return "UserClickLog{" +
                "userId=" + getUserId() +
                ", product_id=" + productId+
                ", category_id=" + categoryId +
                ", province=" + province+
                ", channel=" + channel +
                ", eventTime=" + eventTime +
                '}';
    }

}
