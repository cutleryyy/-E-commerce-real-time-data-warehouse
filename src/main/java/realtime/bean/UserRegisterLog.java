package realtime.bean;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

public class UserRegisterLog implements Serializable {
    @JsonProperty("user_id")
    private Long userId;
    @JsonProperty("user_name")
    private String userName;
    private Integer age;
    private Integer gender;   // 0-女，1-男
    private String province;
    @JsonProperty("vip_level")
    private Integer vipLevel;
    @JsonProperty("timestamp")
    private Long eventTime;

    // 无参构造器（必须）
    public UserRegisterLog() {}

    // Getter / Setter

    public Long getEventTime() {
        return eventTime;
    }

    public void setEventTime(Long eventTime) {
        this.eventTime = eventTime;
    }

    public Integer getVipLevel() {
        return vipLevel;
    }

    public void setVipLevel(Integer vipLevel) {
        this.vipLevel = vipLevel;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public Integer getGender() {
        return gender;
    }

    public void setGender(Integer gender) {
        this.gender = gender;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    @Override
    public String toString() {
        return "UserRegisterLog{user_id=" + userId +
                ", user_name="+ userName +
                ", age=" + age +
                ", gender=" + gender +
                ", province=" + province +
                ", vip_level=" + vipLevel +
                ", timestamp=" + eventTime
                +"}";
    }
}
