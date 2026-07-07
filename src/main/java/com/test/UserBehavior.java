package com.test;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

public class UserBehavior implements Serializable {
        @JsonProperty("order_id")
        private Long orderId;
        @JsonProperty("user_id")
        private Long userId;
        @JsonProperty("action")
        private String action;
        @JsonProperty("product_id")
        private Long productId;
        @JsonProperty("amount")
        private Long amount;   // 用 Double 而非 double，以支持 null
        @JsonProperty("category_id")
        private Long categoryId;
        @JsonProperty("province")
        private String province;
        @JsonProperty("channel")
        private String channel;
        @JsonProperty("is_new_user")
        private Integer isNewUser;
        @JsonProperty("timestamp")
        private Long eventTime;
        // 空参构造
        public UserBehavior(){}

        public Long getEventTime() {
                return eventTime;
        }

        public void setEventTime(Long eventTime) {
                this.eventTime = eventTime;
        }

        public Integer getIsNewUser() {
                return isNewUser;
        }

        public void setIsNewUser(Integer isNewUser) {
                this.isNewUser = isNewUser;
        }

        public String getChannel() {
                return channel;
        }

        public void setChannel(String channel) {
                this.channel = channel;
        }

        public String getProvince() {
                return province;
        }

        public void setProvince(String province) {
                this.province = province;
        }

        public Long getCategoryId() {
                return categoryId;
        }

        public void setCategoryId(Long categoryId) {
                this.categoryId = categoryId;
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
        public String getAction() {
                return action;
        }
        public void setUserId(Long userId) {
                this.userId = userId;
        }
        public Long getProductId() {
                return productId;
        }
        public void setAction(String action) {
                this.action = action;
        }
        public void setProductId(Long productId) {
                this.productId = productId;
        }

        public void setAmount(Long amount) {
                this.amount = amount;
        }

        public Long getAmount() {
                return amount;
        }
                @Override
        public String toString() {
                return "UserBehavior{" +
                        "userId=" + userId +
                        ", action='" + action + '\'' +
                        ", productId=" + productId +
                        ", amount=" + amount +
                        ", timestamp=" + eventTime +
                        '}';
        }
}
