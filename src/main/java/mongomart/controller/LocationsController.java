package mongomart.controller;

import com.mongodb.client.MongoDatabase;
import freemarker.template.Configuration;
import mongomart.config.FreeMarkerEngine;
import mongomart.dao.StoreDao;
import mongomart.model.Store;
import spark.ModelAndView;

import java.util.*;

import static spark.Spark.get;
import static mongomart.config.Utils.isEmpty;
import static mongomart.config.Utils.getIntFromString;

/**
 * Created by gl on 7/16/15.
 */
public class LocationsController {


    /**
     *
     * @param cfg
     * @param storeDatabase
     */
    public LocationsController(Configuration cfg, MongoDatabase storeDatabase) {
        StoreDao storeDao = new StoreDao(storeDatabase);

        get("/locations", (request, response) -> {
            String city = request.queryParams("city");
            String state = request.queryParams("state");
            String zip = request.queryParams("zip");
            String find = request.queryParams("find");

            // Pagination calculations
            int page = getIntFromString(request.queryParams("page"));
            int storesPerPage = 5;
            int skip = storesPerPage * page;
            long numStores = storeDao.countStores();
            int numPages = ((int) Math.ceil(numStores / (double) storesPerPage));

            Map<String, Object> attributes = new HashMap<>();

            List<Store> stores = new ArrayList<>();
            if ("byZip".equals(find)) {
                if (isEmpty(zip)) {
                    attributes.put("zipError", "Please supply a zip.");
                } else {
                    zip = zip.trim();
                    try {
                        stores = storeDao.getStoresClosestToZip(zip, skip, storesPerPage);
                    } catch (StoreDao.ZipNotFound e) {
                        attributes.put("zipError", "Can't find " + zip + " in our database.");
                    }
                }
                city = "";
                state = "";
            } else if ("byCityAndState".equals(find)) {
                if (isEmpty(city) || isEmpty(state)) {
                    attributes.put("cityAndStateError", "Please supply a city and state.");
                } else {
                    city = city.trim();
                    state = state.trim();
                    try {
                        stores = storeDao.getStoresClosestToCityAndState(
                                city, state, skip, storesPerPage);
                    } catch (StoreDao.CityAndStateNotFound e) {
                        attributes.put(
                                "cityAndStateError", "Can't find " + city + ", " + state + " in our database.");
                    }
                }
                zip = "";
            }
            SortedSet<String> states = storeDao.getAllStates();
            attributes.put("stores", stores);
            attributes.put("numStores", numStores);
            attributes.put("find", find);
            attributes.put("states", states);
            attributes.put("city", city);
            attributes.put("state", state);
            attributes.put("zip", zip);
            attributes.put("page", page);
            attributes.put("numPages", numPages);
            return new ModelAndView(attributes, "locations.ftl");
        }, new FreeMarkerEngine(cfg));
    }
}
