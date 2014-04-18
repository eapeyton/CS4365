package gatech.hadoopER.importer;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

/**
 *
 * @author eric
 */
public class ImporterReducer extends Reducer<Text, From, Text, To> {
    private Importer<From,To> importer;
    
    @Override
    protected void setup(Context context) {
        importer = Importer.getInstance(context);
    }
    
    @Override
    public void reduce(Text key, Iterable<From> values, Reducer<Text,From,Text,To>.Context context) throws IOException, InterruptedException {
        ArrayList<To> records = new ArrayList<>();
        for(From value: values) {
            To record = importer.getFromTo(value);
            Logger.getLogger(this.getClass()).info(record.toString());
            context.write(new Text(record.uuid.toString()), record);
        }
    }
    
}
