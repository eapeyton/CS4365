/*
 * CS 4365 Project
 */

package gatech.hadoopER;

import gatech.hadoopER.importer.ImporterXml;
import gatech.hadoopER.io.XMLInputFormat;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

/**
 *
 * @author eric
 */
public class ImportEventsB extends ImporterXml<SampleEventB, GlobalEvent>{
    
    @Override
    protected void map(SampleEventB from, GlobalEvent to) {
        to.title = from.name;
        to.other = from.description;
        if(from.latitude != null & from.longitude != null) {
            to.location = from.latitude + "," + from.longitude;
        }
        else {
            to.location = from.location;
        }
        to.datetime = from.start_time;
    }

    @Override
    protected Class<SampleEventB> getFrom() {
        return SampleEventB.class;
    }

    @Override
    protected Class<GlobalEvent> getTo() {
        return GlobalEvent.class;
    }
    
    @Override
    public Job createJob(Configuration conf) throws IOException {
        Job job = super.createJob(conf);
        job.getConfiguration().set(XMLInputFormat.START_TAG_KEY, "<event>");
        job.getConfiguration().set(XMLInputFormat.END_TAG_KEY, "</event>");
        return job;
    }
    
}
