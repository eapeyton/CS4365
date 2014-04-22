/*
 * CS 4365 Project
 */

package gatech.hadoopER;

import org.apache.hadoop.io.ArrayWritable;

/**
 *
 * @author eric
 */
public class GEArrayWritable extends ArrayWritable {

    public GEArrayWritable() {
        super(GlobalEvent.class);
    }
    
}
