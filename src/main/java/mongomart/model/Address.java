package mongomart.model;

import org.bson.types.ObjectId;

/**
 * Created by glowe on 10/27/15.
 */
public class Address {
    private final String address;
    private final String address2;
    private final String city;
    private final String state;
    private final String zip;
    private final String country;

    public Address(final String address, final String address2, final String city,
                 final String region, final String zip, final String country) {
        this.address = address;
        this.address2 = address2;
        this.city = city;
        this.state = region;
        this.zip = zip;
        this.country = country;
    }

    public String getAddress() {
        return address;
    }

    public String getAddress2() {
        return address2;
    }

    public String getCity() {
        return city;
    }

    public String getState() {
        return state;
    }

    public String getZip() {
        return zip;
    }

    public String getCountry() {
        return country;
    }
}
