package lemongrenade.core.models;

import org.mongodb.morphia.annotations.Entity;
import org.mongodb.morphia.annotations.Id;

import java.util.ArrayList;
import java.util.HashMap;


@Entity(value = "adapterURLs", noClassnameStored = true)

public class LGAdapterURLs {

    @Id
    private String adapterName;
    private String baseUrl;

    /**
     * Constructors
     */

    public LGAdapterURLs() {

    }

    public LGAdapterURLs(String adapterName, String baseURL) {
        this.adapterName = adapterName;
        this.baseUrl = baseURL;
    }

    public String getAdapterName() {
        return adapterName;
    }

    public void setAdapterName(String adapterName) {
        this.adapterName = adapterName;
    }

    public String getBaseUrl() {
        return baseUrl;
    }

    public void setBaseUrl(String baseUrl) {
        this.baseUrl = baseUrl;
    }
}


