package mongomart.dao;

import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import com.mongodb.client.model.UpdateOptions;
import java.util.ArrayList;
import java.util.List;
import mongomart.model.Cart;
import mongomart.model.Item;
import org.bson.Document;

/**
 * All database access to the "cart" collection
 *
 * ALL T O D O BLOCKS MUST BE COMPLETED
 */
public class CartDao {
    // TODO-lab1 this variable must be assigned in constructor
    private final MongoCollection<Document> cartCollection;

    /**
     *
     * @param mongoMartDatabase
     */
    public CartDao(final MongoDatabase mongoMartDatabase) {

        /**
         * TODO-lab1
         *
         * LAB #1: Get the "cart" collection and assign it to cartCollection variable here
         *
         */
        cartCollection = mongoMartDatabase.getCollection("cart"); // TODO-lab1 replace this line
    }

    /**
     * Get a cart by userid
     *
     * @param userid
     * @return
     */
    public Cart getCart(String userid) {

        /**
         * TODO-lab2
         *
         * LAB #2: Query the "cart" collection by userid and return a Cart object
         */

        return docToCart(cartCollection.find(eq("userid", userid)).first());
    }

    /**
     * Add an item to a cart
     *
     * @param item
     * @param userid
     */

    public void addToCart(Item item, String userid) {

        /**
         * TODO-lab2
         *
         * LAB #2: Add an item to a user's cart document
         *
         * HINT: There are several cases you must account for here, such as an empty initial cart
         */
        if(existsCart(item.getId(), userid)){
            cartCollection.updateOne(and(eq("userid", userid), eq("items._id", item.getId())),
                    new Document("$inc", new Document("items.$.quantity", 1)));
        }
        else{
            Document push = new Document("$push", new Document("items", 
                new Document("_id", item.getId())
                .append("title", item.getTitle())
                .append("category", item.getCategory())
                .append("price", item.getPrice())
                .append("quantity", item.getQuantity())
                .append("img_url", item.getImg_url())
            ));
            cartCollection.updateOne(eq("userid", userid), push, new UpdateOptions().upsert(true));
        }

    }

    /**
     * Update the quantity of an item in a cart.  If quantity is 0, remove item from cart
     *
     * @param itemid
     * @param quantity
     * @param userid
     */
    public void updateQuantity(int itemid, int quantity, String userid) {

        /**
         * TODO-lab2
         *
         * LAB #2: Update the quantity of an item in a users cart, if the quantity is 0, remove the item from the cart
         *
         * HINT: You may want to create a helper method for determining if an item already exists in a cart
         */
        if(quantity > 0){
            cartCollection.withWriteConcern(WriteConcern.MAJORITY).updateOne(and(eq("userid", userid), eq("item._id", itemid)),
                new Document("$set", new Document("item.$.quantity", quantity))
            );
        }
        else{
            cartCollection.updateOne(eq("userid", userid),
                new Document("$pull", new Document("items", new Document("_id", itemid)))
            );
        }

    }
    
    private Cart docToCart(Document document){
        Cart cart = new Cart();
        cart.setId(document.getObjectId("_id"));
        cart.setStatus(document.getString("status"));
        cart.setLast_modified(document.getDate("last_modified"));
        cart.setUserid(document.getString("userid"));
        if(document.containsKey("items") && document.get("items") instanceof List ){
            List<Item> items = new ArrayList<>();
            List<Document> itemList = (List<Document>) document.get("items");
            for (Document itemDoc : itemList) {
                Item item = new Item();
                if(itemDoc.containsKey("_id")){
                    item.setId(itemDoc.getInteger("_id"));
                }
                if(itemDoc.containsKey("quantity")){
                    item.setQuantity(itemDoc.getInteger("quantity"));
                }
                if(itemDoc.containsKey("title")){
                    item.setTitle(itemDoc.getString("title"));
                }
                if(itemDoc.containsKey("img_url")){
                    item.setImg_url(itemDoc.getString("img_url"));
                }
                if(itemDoc.containsKey("price")){
                    item.setPrice(itemDoc.getDouble("price"));
                }
                items.add(item);
            }
            cart.setItems(items);
        }else{
            cart.setItems(new ArrayList<>());
        }
        return cart;
    }

    private boolean existsCart(int itemid, String userid){
        long found = cartCollection.count(and(eq("userid", userid), eq("items._id", itemid)));
        return found > 0;
    }
}
