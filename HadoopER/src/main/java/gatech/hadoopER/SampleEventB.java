/*
 * CS 4365 Project
 */

package gatech.hadoopER;

import gatech.hadoopER.importer.From;

/**
 *
 * @author eric
 */
public class SampleEventB extends From {
    long id;
    String name;
    String description;
    String location;
    String owner;
    String start_time;
    String timezone;
    String venue;
    String latitude;
    String longitude;

    @Override
    public String getKey() {
        return Long.toString(id);
    }
    
}
