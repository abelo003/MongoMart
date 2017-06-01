package mongomart.model;

import org.bson.types.ObjectId;

/**
 * Created by glowe on 10/27/15.
 */
public class Geo {
    private final double latitude;
    private final double longitude;

    public Geo(final double longitude, final double latitude) {
        this.longitude = longitude;
        this.latitude = latitude;
    }

    public double getLatitude() {
        return latitude;
    }

    public double getLongitude() {
        return longitude;
    }
}
