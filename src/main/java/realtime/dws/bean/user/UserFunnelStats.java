package realtime.dws.bean.user;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

public class UserFunnelStats implements Serializable {
    @JsonProperty("window_start")
    private Long windowStart;
    private Long windowEnd;
    private Long registerCount;
    private Long convertedCount;
    private Boolean isConverted;

    public UserFunnelStats(){}

    public Boolean getConverted() {
        return isConverted;
    }
    public Boolean getIsConverted() {
        return isConverted;
    }


    public void setConverted(Boolean converted) {
        isConverted = converted;
    }

    public Long getWindowStart() {
        return windowStart;
    }

    public void setWindowStart(Long windowStart) {
        this.windowStart = windowStart;
    }

    public Long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(Long windowEnd) {
        this.windowEnd = windowEnd;
    }

    public Long getRegisterCount() {
        return registerCount;
    }

    public void setRegisterCount(Long registerCount) {
        this.registerCount = registerCount;
    }

    public Long getConvertedCount() {
        return convertedCount;
    }

    public void setConvertedCount(Long convertedCount) {
        this.convertedCount = convertedCount;
    }
}
