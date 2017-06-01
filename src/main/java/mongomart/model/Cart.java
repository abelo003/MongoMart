package mongomart.model;

import mongomart.controller.CartController;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Cart Model object
 */
public class Cart {
    ObjectId id;
    String status;
    Date last_modified;
    String userid;
    List<Item> items;

    public Cart() {
    }

    public ObjectId getId() {
        return id;
    }

    public void setId(ObjectId id) {
        this.id = id;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Date getLast_modified() {
        return last_modified;
    }

    public void setLast_modified(Date last_modified) {
        this.last_modified = last_modified;
    }

    public String getUserid() {
        return userid;
    }

    public void setUserid(String userid) {
        this.userid = userid;
    }

    public List<Item> getItems() {
        return items;
    }

    public void setItems(List<Item> items) {
        this.items = items;
    }

    /**
     * Create a fake Cart object for testing
     *
     * @return mock Cart object
     */
    public static Cart getMockCart() {
        Cart cart = new Cart();
        cart.setId(new ObjectId());
        cart.setStatus("active");
        cart.setLast_modified(new Date());
        cart.setUserid(CartController.USERID);

        Item item = Item.getMockItem();
        List<Item> items = new ArrayList<>();
        items.add(item);

        cart.setItems(items);
        return cart;
    }
}
