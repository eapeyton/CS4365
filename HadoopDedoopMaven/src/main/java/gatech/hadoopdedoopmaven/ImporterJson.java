package gatech.hadoopdedoopmaven;

import java.io.IOException;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;

/**
 *
 * @author eric
 */
public abstract class ImporterJson<F extends From,T extends To> extends Importer<F,T> {

    @Override
    protected void writableToFrom(Writable writable, F from) {
        MapWritable mWritable = (MapWritable)writable;
        mapToFrom(mWritable,from);
    }
    
    protected abstract void mapToFrom(MapWritable value, F from);
            
    @Override
    protected Job createJob() throws IOException {
        Job job = super.createJob();
        job.setInputFormatClass(JsonInputFormat.class);
        return job;
    }
    
}
