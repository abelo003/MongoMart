package mongomart.controller;

import com.mongodb.client.MongoDatabase;
import freemarker.template.Configuration;
import mongomart.config.FreeMarkerEngine;
import mongomart.config.Utils;
import mongomart.dao.ItemDao;
import mongomart.model.Category;
import mongomart.model.Item;
import spark.ModelAndView;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static spark.Spark.get;

/**
 * MongoMart store/item controller
 *
 * Provide functionality to:
 *      - View an item
 *      - View items in a category
 *      - Add a review
 *      - Text search for items
 */
public class StoreController {

    /**
     *
     * @param cfg
     * @param itemDatabase
     */
    public StoreController(Configuration cfg, MongoDatabase itemDatabase) {

        ItemDao itemDao = new ItemDao(itemDatabase);

        // Homepage and category search
        get("/", (request, response) -> {
            /**
             * TODO-lab3
             *
             * LAB #3: Set this variable to true, implement all other code changes

             */
            boolean useRangeBasedPagination = false;


            String category = request.queryParams("category");
            String page = request.queryParams("page");

            List<Item> items = new ArrayList<>();
            List<Category> categories = itemDao.getCategoriesAndNumProducts();
            long itemCount = 0;

            // Search by category
            if (category != null && (!category.equals("All") && !category.trim().equals(""))) {
                items = itemDao.getItemsByCategory(category, page);
                itemCount = itemDao.getItemsByCategoryCount(category);
            }
            // Else show all items
            else {
                /**
                 * TODO-lab3
                 *
                 * LAB #3: Create a new method in the ItemDao class for getting items based on a range, replace the
                 * line below with this new method
                 *
                 * HINT: You may want to use before/after variables passed in from the page to help define the ranges.
                 * (however, please use any method you feel most comfortable with)
                 * E.g. String before = request.queryParams("before"); String after = request.queryParams("after");
                 *
                 */
                items = itemDao.getItems(page);

                itemCount = itemDao.getItemsCount();
                category = "All";
            }

            // Determine the number of pages to display in the UI (pagination)
            int num_pages = 0;
            if (itemCount > itemDao.getItemsPerPage()) {
                num_pages = (int)Math.ceil(itemCount / itemDao.getItemsPerPage());
            }

            HashMap<String, Object> attributes = new HashMap<>();

            /**
             * TODO-lab3
             *
             * LAB #3: The nextPageUrl and previousPageUrl attributes are used by home.ftl for displaying previous and
             * next page.  If either is null, previous/next page will not be displayed
             *
             * HINT: make sure to only set nextPageUrl or previousPageUrl when you want them to be displayed in the UI
             *
             */
            if (useRangeBasedPagination) {
                attributes.put("nextPageUrl", "/#");
                attributes.put("previousPageUrl", "/#");
            }

            attributes.put("useRangeBasedPagination", useRangeBasedPagination);
            attributes.put("items", items);
            attributes.put("item_count", itemCount);
            attributes.put("categories", categories);
            attributes.put("category_param", category);
            attributes.put("page", Utils.getIntFromString(page));
            attributes.put("num_pages", num_pages);

            return new ModelAndView(attributes, "home.ftl");
        }, new FreeMarkerEngine(cfg));

        // View an item
        get("/item", (request, response) -> {
            String itemid = request.queryParams("id");
            HashMap<String, Object> attributes = buildItemResponse(itemid, itemDao);
            return new ModelAndView(attributes, "item.ftl");
        }, new FreeMarkerEngine(cfg));

        // Add a review for an item
        get("/add-review", (request, response) -> {
            String itemid = request.queryParams("itemid");
            String review = request.queryParams("review");
            String name = request.queryParams("name");
            String stars = request.queryParams("stars");

            itemDao.addReview(itemid, review, name, stars);

            HashMap<String, Object> attributes = buildItemResponse(itemid, itemDao);
            return new ModelAndView(attributes, "item.ftl");
        }, new FreeMarkerEngine(cfg));

        // Text search for an item, requires a text index
        get("/search", (request, response) -> {
            String query = request.queryParams("query");
            String page = request.queryParams("page");

            List<Item> items = itemDao.textSearch(query, page);
            long itemCount = itemDao.textSearchCount(query);

            // Determine the number of pages to display in the UI (pagination)
            int num_pages = 0;
            if (itemCount > itemDao.getItemsPerPage()) {
                num_pages = (int)Math.ceil(itemCount / itemDao.getItemsPerPage());
            }

            HashMap<String, Object> attributes = new HashMap<>();
            attributes.put("items", items);
            attributes.put("item_count", itemCount);
            attributes.put("query_string", query);
            attributes.put("page", Utils.getIntFromString(page));
            attributes.put("num_pages", num_pages);

            return new ModelAndView(attributes, "search.ftl");
        }, new FreeMarkerEngine(cfg));

    }

    /**
     * Construct the necessary UI attributes to render an Item page
     *
     * @param itemid
     * @param itemDao
     * @return
     */
    private HashMap<String, Object> buildItemResponse(String itemid, ItemDao itemDao) {
        List<Item> related_items = itemDao.getItems("0");
        Item item = itemDao.getItem(new Integer(itemid));

        HashMap<String, Object> attributes = new HashMap<>();
        attributes.put("item", item);
        attributes.put("itemid", itemid);
        attributes.put("related_items", related_items.subList(0, 4));
        return attributes;
    }
}
