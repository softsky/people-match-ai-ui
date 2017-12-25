package com.localblox.ashfaq.ui.storage;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("storage")
public class StorageProperties {

    /**
     * Folder location for storing files
     */
    private String location = "hdfs://master/in";

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

}
