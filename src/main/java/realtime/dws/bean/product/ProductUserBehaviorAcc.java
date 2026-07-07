package realtime.dws.bean.product;

public class ProductUserBehaviorAcc {
    public long click;
    public long view;
    public long cart;
    public long userId;
    public long productId;
    public ProductUserBehaviorAcc(){}

    public long getProductId() {
        return productId;
    }

    public void setProductId(long productId) {
        this.productId = productId;
    }

    public long getUserId() {
        return userId;
    }

    public void setUserId(long userId) {
        this.userId = userId;
    }

    public long getCart() {
        return cart;
    }

    public void setCart(long cart) {
        this.cart = cart;
    }

    public long getView() {
        return view;
    }

    public void setView(long view) {
        this.view = view;
    }

    public long getClick() {
        return click;
    }

    public void setClick(long click) {
        this.click = click;
    }
}
