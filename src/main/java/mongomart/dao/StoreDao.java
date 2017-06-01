package mongomart.dao;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import mongomart.model.Address;
import mongomart.model.Geo;
import mongomart.model.Store;
import org.bson.Document;

import java.util.*;
import java.util.stream.Collectors;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Sorts.orderBy;
import static com.mongodb.client.model.Sorts.ascending;

/**
 * Created by glowe on 6/22/15.
 */
public class StoreDao {
    private final MongoCollection<Document> collection;
    private final MongoCollection<Document> zipCollection;
    private final MongoDatabase database;

    public static class ZipNotFound extends Exception {
        public final String zip;

        public ZipNotFound(final String zip) {
            super();
            this.zip = zip;
        }
    }

    public static class CityAndStateNotFound extends Exception {
        public final String city;
        public final String state;

        public CityAndStateNotFound(final String city, final String state) {
            super();
            this.city = city;
            this.state = state;
        }
    }

    /**
     *
     * @param mongoMartDatabase
     */
    public StoreDao(final MongoDatabase mongoMartDatabase) {
        database = mongoMartDatabase;
        collection = database.getCollection("store");
        zipCollection = database.getCollection("zip");
    }

    public SortedSet<String> getAllStates() {
        try (MongoCursor<String> cursor = zipCollection.distinct("state", String.class).iterator()) {
            SortedSet<String> states = new TreeSet<>();
            while (cursor.hasNext()) {
                states.add(cursor.next());
            }
            return states;
        }
    }

    public List<Store> getStoresClosestToCityAndState(
            final String city, final String state, final int skip, final int limit)
            throws CityAndStateNotFound {
        // Arbitrarily choose a matching location.
        Document doc = zipCollection.find(
                and(eq("city", city.toUpperCase()), eq("state", state))).first();
        if (doc == null) {
            throw new CityAndStateNotFound(city, state);
        }
        List<Double> location = (List<Double>) doc.get("loc");
        return getStoresClosestToLocation(location.get(0), location.get(1), skip, limit);
    }

    public List<Store> getStoresClosestToZip(
            final String zipCode, final int skip, final int limit) throws ZipNotFound {
        Document doc = zipCollection.find(eq("_id", zipCode)).first();
        if (doc == null) {
            throw new ZipNotFound(zipCode);
        }
        List<Double> location = (List<Double>) doc.get("loc");
        return getStoresClosestToLocation(location.get(0), location.get(1), skip, limit);
    }

    private List<Store> getStoresClosestToLocation(
             final double longitude, final double latitude, final int skip, final int limit) {
        // TODO-lab6

        // Use the $geoNear aggregration operator to query for stores
        // in order of proximity to the specified point (i.e.,
        // longitude and latitude), skipping over `skip` documents and
        // limiting results to `limit`. Ensure that your document has
        // a computed field `distanceFromPoint` (see docToStore).

        List<Document> docs = new ArrayList<>();

        // TODO-lab6 Replace code above.

        return docsToStores(docs);
    }

    public long countStores() {
        return collection.count();
    }

    private static Store docToStore(Document document) {
        List<Double> coords = (List<Double>) document.get("coords");
        double longitude = coords.get(0);
        double latitude = coords.get(1);
        Geo geo = new Geo(longitude, latitude);
        Address address = new Address(
            document.getString("address"), document.getString("address2"),
            document.getString("city"), document.getString("state"),
            document.getString("zip"), document.getString("country"));
        
        double distanceFromPoint = document.containsKey("distanceFromPoint") ?
                document.getDouble("distanceFromPoint") : 0.0;

        return new Store(
                document.getObjectId("_id"), document.getString("storeId"), document.getString("name"),
                geo, address, distanceFromPoint);
    }

    private static List<Store> docsToStores(List<Document> documents) {
        return documents.stream().map(StoreDao::docToStore).collect(Collectors.toList());
    }
}
