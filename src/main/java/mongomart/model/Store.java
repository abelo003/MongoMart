package mongomart.model;

import org.bson.types.ObjectId;

/**
 * Created by glowe on 6/19/15.
 */
public class Store {
    private final ObjectId id;
    private final String storeId;
    private final String name;
    private final Geo geo;
    private final Address address;
    private final double distanceFromPoint;

    public Store(final ObjectId id, final String storeId, final String name,
                 final Geo geo, final Address address,
                 final double distanceFromPoint) {
        this.id = id;
        this.storeId = storeId;
        this.name = name;
        this.geo = geo;
        this.address = address;
        this.distanceFromPoint = distanceFromPoint;
    }

    public ObjectId getId() {
        return id;
    }

    public String getStoreId() {
        return storeId;
    }

    public String getName() {
        return name;
    }

    public double getLatitude() {
        return geo.getLatitude();
    }

    public double getLongitude() {
        return geo.getLongitude();
    }

    public String getAddress() {
        return address.getAddress();
    }

    public String getAddress2() {
        return address.getAddress2();
    }

    public String getCity() {
        return address.getCity();
    }

    public String getState() {
        return address.getState();
    }

    public String getZip() {
        return address.getZip();
    }

    public String getCountry() {
        return address.getCountry();
    }

    public double getDistanceFromPoint() {
        return distanceFromPoint;
    }
}
