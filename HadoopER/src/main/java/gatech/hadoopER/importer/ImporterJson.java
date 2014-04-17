package gatech.hadoopER.importer;

import gatech.hadoopER.io.JsonInputFormat;
import java.util.HashMap;
import java.util.Map.Entry;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;

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
        from.readMap(map);
    }
            
    @Override
    protected Class<? extends InputFormat> getInputFormat() {
        return JsonInputFormat.class;
    }
    
}
