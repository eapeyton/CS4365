/*
 * CS 4365 Project
 */
package gatech.hadoopER;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

/**
 *
 * @author eric
 */
public interface ERJob {

    public Job createJob(Configuration conf) throws IOException;
}
