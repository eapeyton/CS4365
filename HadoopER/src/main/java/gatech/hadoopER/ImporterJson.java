package gatech.hadoopER;

import gatech.hadoopER.io.JsonInputFormat;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;
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
        Field[] fields = from.getClass().getDeclaredFields();
        HashMap<String,String> map = new HashMap<>();
        for(Entry<Writable,Writable> entry: mWritable.entrySet()) {
            map.put(entry.getKey().toString(), entry.getValue().toString());
        }
        for(Field field: fields) {
            try {
                field.setAccessible(true);
                field.set(from, map.get(field.getName()));
            } catch (IllegalArgumentException | IllegalAccessException ex) {
                Logger.getLogger(ImporterJson.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
            
    @Override
    protected Job createJob() throws IOException {
        Job job = super.createJob();
        job.setInputFormatClass(JsonInputFormat.class);
        return job;
    }
    
}
