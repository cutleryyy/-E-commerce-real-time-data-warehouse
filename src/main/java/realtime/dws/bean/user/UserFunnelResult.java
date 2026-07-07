package realtime.dws.bean.user;
// 注册流 -》 订单支付流 30分钟内转化率
public class UserFunnelResult {
    private Long userId;
    private Long registerTime;
    private Long orderTime;
    public Boolean isConverted; // 是否30分钟内转化

    public UserFunnelResult(){}

    public Boolean getConverted() {
        return isConverted;
    }

    public void setConverted(Boolean converted) {
        isConverted = converted;
    }

    public Long getOrderTime() {
        return orderTime;
    }

    public void setOrderTime(Long orderTime) {
        this.orderTime = orderTime;
    }

    public Long getRegisterTime() {
        return registerTime;
    }

    public void setRegisterTime(Long registerTime) {
        this.registerTime = registerTime;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    @Override
    public String toString() {
        return "UserFunnelResult:{" +
                "userId=" + userId+
                ", registerTime=" + registerTime +
                ", orderTime=" + orderTime +
                ", isConverted=" + isConverted+
                "}" ;
    }
}
