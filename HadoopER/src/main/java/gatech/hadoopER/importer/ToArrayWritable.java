/*
 * CS 4365 Project
 */

package gatech.hadoopER.importer;

import org.apache.hadoop.io.ArrayWritable;

/**
 *
 * @author eric
 */
public class ToArrayWritable extends ArrayWritable {
    public ToArrayWritable() {
        super(To.class);
    }
}
