package mongomart;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoDatabase;
import freemarker.template.Configuration;
import mongomart.controller.AdminController;
import mongomart.controller.CartController;
import mongomart.controller.LocationsController;
import mongomart.controller.StoreController;

import java.io.IOException;
import java.util.Arrays;

import static spark.SparkBase.port;
import static spark.SparkBase.staticFileLocation;

/**
 * Main MongoMart class
 *
 * To run:
 *      Ensure the dataset from data/ has been imported to your MongoDB instance (instructions located in README.md)
 *      Run the main method below
 *      Open http://localhost:8080
 */
public class MongoMart {
    public static final int HTTP_PORT = 8082;  // HTTP port to listen for requests on

    /**
     * Main method for MongoMart application
     *
     * @param args
     * @throws IOException
     */

    public static void main(String[] args) throws IOException {
        if (args.length == 0) {
            new MongoMart("mongodb://Red-Mac-2.local:27017,Red-Mac-2.local:27018,Red-Mac-2.local:27019");
        }
        else {
            new MongoMart(args[0]);
        }
    }

    /**
     * Create an instance of MongoMart
     *
     * @param mongoURIString
     * @throws IOException
     */

    public MongoMart(String mongoURIString) throws IOException {
        /**
         * TODO-lab1
         *
         * LAB #1: Create a connection to your MongoDB instance, assign the "mongomart"
         *         database to the itemDatabase variable
         *
         * HINT: You'll need to create a MongoClient object first, the "mongoURIString" may also be used
         *
         */
        
//        MongoClientOptions options = MongoClientOptions.builder().writeConcern(WriteConcern.MAJORITY).build();
        
        MongoClientURI repSetURI = new MongoClientURI(mongoURIString, MongoClientOptions.builder().writeConcern(WriteConcern.MAJORITY));
        
        MongoClient client = new MongoClient(repSetURI);
        final MongoDatabase itemDatabase = client.getDatabase("mongomart");

        /**
         * TODO-lab1 Replace all code above
         */

        // Freemarker configuration
        final Configuration cfg = createFreemarkerConfiguration();

        port(HTTP_PORT);
        staticFileLocation("/assets");

        // Start controllers
        AdminController adminController = new AdminController(cfg, itemDatabase);
        StoreController storeController = new StoreController(cfg, itemDatabase);
        CartController cartController = new CartController(cfg, itemDatabase);
        LocationsController locationsController = new LocationsController(cfg, itemDatabase);
    }

    /**
     *
     * @return
     */
    private Configuration createFreemarkerConfiguration() {
        Configuration retVal = new Configuration();
        retVal.setClassForTemplateLoading(MongoMart.class, "/freemarker");
        return retVal;
    }
}
