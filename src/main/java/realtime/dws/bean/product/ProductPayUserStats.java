package realtime.dws.bean.product;

import realtime.dws.bean.base.BaseTradeStats;

public class ProductPayUserStats extends BaseTradeStats {
    private Long productId;
    public ProductPayUserStats(){}

    @Override
    public void setWindowStart(Long windowStart) {
        super.setWindowStart(windowStart);
    }

    @Override
    public void setWindowEnd(Long windowEnd) {
        super.setWindowEnd(windowEnd);
    }

    @Override
    public Long getWindowStart() {
        return super.getWindowStart();
    }

    @Override
    public void setOldUserCount(Long oldUserCount) {
        super.setOldUserCount(oldUserCount);
    }

    @Override
    public void setGmv(Long gmv) {
        super.setGmv(gmv);
    }

    @Override
    public Long getWindowEnd() {
        return super.getWindowEnd();
    }

    @Override
    public Long getOrderCount() {
        return super.getOrderCount();
    }

    @Override
    public void setPayUserCount(Long payUserCount) {
        super.setPayUserCount(payUserCount);
    }

    @Override
    public Long getOldUserCount() {
        return super.getOldUserCount();
    }

    @Override
    public Long getNewUserCount() {
        return super.getNewUserCount();
    }

    @Override
    public Long getPayUserCount() {
        return super.getPayUserCount();
    }

    @Override
    public void setNewUserCount(Long newUserCount) {
        super.setNewUserCount(newUserCount);
    }

    public Long getProductId() {
        return productId;
    }

    public void setProductId(Long productId) {
        this.productId = productId;
    }

}
