package gatech.hadoopdedoopmaven;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
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
        HashMap<String,String> map = new HashMap<>();
        for(Entry<Writable,Writable> entry: mWritable.entrySet()) {
            map.put(entry.getKey().toString(), entry.getValue().toString());
        }
        mapToFrom(map,from);
    }
    
    protected abstract void mapToFrom(Map<String,String> value, F from);
            
    @Override
    protected Job createJob() throws IOException {
        Job job = super.createJob();
        job.setInputFormatClass(JsonInputFormat.class);
        return job;
    }
    
}
