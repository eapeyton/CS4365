/*
 * CS 4365 Project
 */
package gatech.hadoopER.importer;

import gatech.hadoopER.events.GlobalEvent;
import org.apache.hadoop.io.ArrayWritable;

/**
 *
 * @author eric
 */
public class ToArrayWritable extends ArrayWritable {

    public ToArrayWritable() {
        super(GlobalEvent.class);
    }
}
