package realtime.dws.bean.total;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

public class PayUserCountStats {
    @JsonProperty("window_start")
    private Long windowStart;
    private Long windowEnd;
    private Long payUserCount;
    public PayUserCountStats(){}

    public Long getPayUserCount() {
        return payUserCount;
    }

    public void setPayUserCount(Long payUserCount) {
        this.payUserCount = payUserCount;
    }

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
//        return "UserCount{" +
//                "windowStart=" + windowStart +
//                ", windowEnd=" + windowEnd +
//                ", userCount=" + payUserCount;
//    }
}
