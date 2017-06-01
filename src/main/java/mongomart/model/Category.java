package mongomart.model;

/**
 * Category model object
 */
public class Category {
    String name;
    int num_items;

    public Category() {

    }

    public Category (String name, int num_items) {
        this.name = name;
        this.num_items = num_items;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getNum_items() {
        return num_items;
    }

    public void setNum_items(int num_items) {
        this.num_items = num_items;
    }
}
