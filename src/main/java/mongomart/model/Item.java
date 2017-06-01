package mongomart.model;

import java.util.ArrayList;
import java.util.List;

/**
 * Item model object
 */
public class Item {
    int id = 0;
    String title;
    String description;
    String category;
    double price;
    int stars;
    String img_url;
    String slogan;
    List<Review> reviews;
    int quantity;
    int num_reviews;

    public Item() {

    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public List<Review> getReviews() {
        return reviews;
    }

    public void setReviews(List<Review> reviews) {
        this.reviews = reviews;
    }

    public int getStars() {
        return stars;
    }

    public void setStars(int stars) {
        this.stars = stars;
    }

    public String getImg_url() {
        return img_url;
    }

    public void setImg_url(String img_url) {
        this.img_url = img_url;
    }

    public String getSlogan() {
        return slogan;
    }

    public void setSlogan(String slogan) {
        this.slogan = slogan;
    }

    public int getQuantity() {
        return quantity;
    }

    public void setQuantity(int quantity) {
        this.quantity = quantity;
    }

    public int getNum_reviews() { return num_reviews; }

    public void setNum_reviews(int num_reviews) { this.num_reviews = num_reviews; }

    public static Item getMockItem() {
        Item item = new Item();
        item.setTitle("Lorem ipsum dolor sit amet");
        item.setDescription("Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.");
        item.setCategory("Apparel");
        item.setPrice(9.99);
        item.setImg_url("/img/products/hoodie.jpg");
        item.setSlogan("Lorem ipsum");
        item.setStars(4);
        item.setReviews(new ArrayList<>());
        return item;
    }
}
