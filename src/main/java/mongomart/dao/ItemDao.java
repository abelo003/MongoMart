package mongomart.dao;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.text;
import mongomart.model.Category;
import mongomart.model.Item;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import mongomart.config.Utils;
import mongomart.model.Review;
import org.bson.BsonDocument;
import org.bson.BsonValue;

/**
 * All database access to the "item" collection
 *
 * ALL T O D O BLOCKS MUST BE COMPLETED
 */
public class ItemDao {
    // TODO-lab1 this variable must be assigned in constructor
    private final MongoCollection<Document> itemCollection;

    private static final int ITEMS_PER_PAGE = 5;

    /**
     *
     * @param mongoMartDatabase
     */
    public ItemDao(final MongoDatabase mongoMartDatabase) {
        /**
         * TODO-lab1
         *
         * LAB #1: Get the "item" collection and assign it to itemCollection variable here
         *
         */
        itemCollection = mongoMartDatabase.getCollection("item"); // TODO-lab1 replace this line

    }

    /**
     * Get an Item by id
     *
     * @param id
     * @return
     */
    public Item getItem(int id) {
        /**
         * TODO-lab2
         *
         * LAB #2: Query the "item" collection by _id and return a Cart object
         */
        Document doc = itemCollection.find(eq("_id", id)).first();
        Item item = docToItem(doc);
        
        //calcula el numero de estrellas
        if(null != item.getReviews()){
            int numReviews = 0;
            int totalStars = 0;
            for (Review col : item.getReviews()) {
                numReviews ++;
                totalStars += col.getStars();
            }
            if(numReviews > 0){
                item.setStars((int) totalStars / numReviews);
            }
        }
        
        return item;
    }

    /**
     * Get items by page number
     *
     * @param page_str the page number the user is currently on, starting at 0
     * @return
     */
    public List<Item> getItems(String page_str) {

        /**
         * TODO-lab2
         *
         * LAB #2: Return a list of items from the "item" collection, limit by ITEMS_PER_PAGE and
         *         skip by (page_str * ITEMS_PER_PAGE)
         *
         * HINT: page_str is a string,
         */

        int page = Utils.getIntFromString(page_str);
        List<Document> docs = itemCollection.find().skip(ITEMS_PER_PAGE * page).limit(ITEMS_PER_PAGE).into(new ArrayList<>());

        return docToItem(docs);
        /**
         * TODO-lab2 Replace all code above
         */

    }

    /**
     * Get number of items, useful for pagination
     *
     * @return
     */
    public long getItemsCount() {

        /**
         * TODO-lab2
         *
         * LAB #2: Count the items in the "item" collection, used for pagination
         *
         */

        /**
         * TODO-lab2 Replace all code above
         */

        return itemCollection.count();
    }

    /**
     * Get items by category, and page (starting at 0)
     *
     * @param category
     * @param page_str
     * @return
     */
    public List<Item> getItemsByCategory(String category, String page_str) {

        /**
         * TODO-lab2
         *
         * LAB #2: Return a list of items from the "item" collection by category, limit by ITEMS_PER_PAGE and
         *         skip by (page_str * ITEMS_PER_PAGE)
         *
         * HINT: page_str is a string,
         */

        int page = Utils.getIntFromString(page_str);
        List<Document> docs = itemCollection.find(eq("category", category)).skip(ITEMS_PER_PAGE * page).limit(ITEMS_PER_PAGE).into(new ArrayList<>());

        
        /**
         * TODO-lab2 Replace all code above
         */

        return docToItem(docs);
    }

    /**
     * Get number of items in a category, useful for pagination
     *
     * @param category
     * @return
     */
    public long getItemsByCategoryCount(String category) {

        /**
         * TODO-lab2
         *
         * LAB #2: Count the items in the "item" collection by category, used for pagination
         *
         */

        /**
         * TODO-lab2 Replace all code above
         */

        return itemCollection.count(eq("category", category));
    }

    /**
     * Text search, requires the index:
     *      db.item.createIndex( { "title" : "text", "slogan" : "text", "description" : "text" } )
     *
     * @param query_str
     * @param page_str
     * @return
     */
    public List<Item> textSearch(String query_str, String page_str) {

        /**
         * TODO-lab2
         *
         * LAB #2: Perform a text search against the item collection, , limit by ITEMS_PER_PAGE and
         *         skip by (page_str * ITEMS_PER_PAGE)
         *
         * HINT: page_str is a string,
         */
        
        int page = Utils.getIntFromString(page_str);

        List<Document> docs = itemCollection.find(text(query_str)).skip(ITEMS_PER_PAGE * page).limit(ITEMS_PER_PAGE).into(new ArrayList<>());

        /**
         * TODO-lab2 Replace all code above
         */

        return docToItem(docs);
    }

    /**
     * Get count for text search results, useful for pagination
     *
     * @param query_str
     * @return
     */
    public long textSearchCount(String query_str) {

        /**
         * TODO-lab2
         *
         * LAB #2: Count the items in the "item" collection based on a text search, used for pagination
         *
         */

        

        /**
         * TODO-lab2 Replace all code above
         */

        return itemCollection.count(text(query_str));
    }

    /**
     * Use aggregation to get a count of the number of products in each category
     *
     * @return
     */
    public List<Category> getCategoriesAndNumProducts() {
        /**
         * TODO-lab2
         *
         * LAB #2: Create an aggregation query to return the total number of items in each category.  The
         *         Category object contains "name" and "num_items".  Remember to include an "All" category
         *         for counting all items in the database.
         *
         * HINT: Test your mongodb query in the shell first before implementing it in Java
         */
        ArrayList<Category> categories = new ArrayList<>();
        Document groupStage = new Document(
            "$group", new Document("_id", "$category")
        .append("num", new Document("$sum", 1)));
        
        Document sortStage = new Document("$sort", new Document("_id", 1));

        List<Document> aggregateStage = new ArrayList<>();
        aggregateStage.add(groupStage);
        aggregateStage.add(sortStage);
        
        MongoCursor<Document> cursor = itemCollection.aggregate(aggregateStage, Document.class).useCursor(true).iterator();
        int totalCout = 0;
        
        while(cursor.hasNext()){
            Document resultDoc = cursor.next();
            Category category = new Category(resultDoc.getString("_id"), resultDoc.getInteger("num"));
            categories.add(category);
            totalCout += resultDoc.getInteger("num");
        }
        categories.add(0, new Category("All", totalCout));
        /**
         * TODO-lab2 Replace all code above
         */

        return categories;
    }

    /**
     * Add a review to an item
     *
     * @param itemid
     * @param review_text
     * @param name
     * @param stars
     */
    public void addReview(String itemid, String review_text, String name, String stars) {
        /**
         * TODO-lab2
         *
         * LAB #2: Add a review to an item document
         *
         * HINT: Remember that reviews are a list within the Item object
         *
         */
        
        Review review = new Review();
        review.setComment(review_text);
        review.setDate(new Date());
        review.setName(name);
        review.setStars(Utils.getIntFromString(stars));
        
        int itemId = Utils.getIntFromString(itemid);
        Document pushUpdate = new Document("$push", new Document("reviews", reviewToDoc(review)));
        
        itemCollection.updateOne(eq("_id", itemId), pushUpdate);
    }

    /**
     * Return the constant ITEMS_PER_PAGE
     *
     * @return
     */
    public int getItemsPerPage() {
        return ITEMS_PER_PAGE;
    }
    
    private Item docToItem(Document doc){
        Item item = new Item();
        item.setId(doc.getInteger("_id"));
        item.setTitle(doc.getString("title"));
        item.setDescription(doc.getString("description"));
        item.setCategory(doc.getString("category"));
        item.setPrice(doc.getDouble("price"));
        item.setStars(doc.getInteger("stars"));
        item.setSlogan(doc.getString("slogan"));
        item.setImg_url(doc.getString("img_url"));
        if(doc.containsKey("quantity")){
            item.setQuantity(doc.getInteger("quantity"));
        }
        if(doc.containsKey("reviews") && doc.get("reviews") instanceof List){
            List<Review> reviews = new ArrayList<>();
            List<Document> reviewsList = (List<Document>) doc.get("reviews");
            for(Document reviewDoc: reviewsList){
                Review review = new Review();
                review.setComment(reviewDoc.getString("comment"));
                review.setName(reviewDoc.getString("name"));
                review.setStars(reviewDoc.getInteger("stars"));
                review.setDate(reviewDoc.getDate("date"));
            }
            item.setReviews(reviews);
        }else{
            item.setReviews(new ArrayList<>());
        }
        
        return item;
    }
    
    private List<Item> docToItem(List<Document> documents){
        List<Item> returnValue = new ArrayList<>();
        for(Document doc : documents){
            returnValue.add(docToItem(doc));
        }
        return returnValue;
    } 
    
    private Document reviewToDoc(Review review){
        Document document = new Document();
        document.append("_id", review.getId());
        document.append("name", review.getName());
        document.append("date", review.getDate());
        document.append("comment", review.getComment());
        document.append("stars", review.getStars());
        return document;
    }
    
}
