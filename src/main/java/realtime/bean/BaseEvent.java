package realtime.bean;

import com.fasterxml.jackson.annotation.JsonProperty;

public class BaseEvent {
    @JsonProperty("user_id")
    protected Long userId;
    @JsonProperty("province")
    protected String province;
    @JsonProperty("timestamp")
    protected Long eventTime;

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public Long getEventTime() {
        return eventTime;
    }

    public void setEventTime(Long eventTime) {
        this.eventTime = eventTime;
    }
}
