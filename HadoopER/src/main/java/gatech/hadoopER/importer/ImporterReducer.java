package gatech.hadoopER.importer;

import java.util.ArrayList;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

/**
 *
 * @author eric
 */
public class ImporterReducer extends Reducer<Text, From, NullWritable, NullWritable> {
    private Importer<From,To> importer;
    
    @Override
    protected void setup(Context context) {
        importer = Importer.getInstance(context);
    }
    
    @Override
    public void reduce(Text key, Iterable<From> values, Reducer<Text,From,NullWritable,NullWritable>.Context context) {
        ArrayList<To> records = new ArrayList<>();
        for(From value: values) {
            To record = importer.getFromTo(value);
            records.add(record);
        }
        for(To record: records) {
            Logger.getLogger(this.getClass()).info(record.toString());
        }
    }
    
}
