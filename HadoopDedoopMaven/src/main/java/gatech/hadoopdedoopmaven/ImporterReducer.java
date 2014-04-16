package gatech.hadoopdedoopmaven;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

/**
 *
 * @author eric
 */
public class ImporterReducer extends Reducer<Text, From, NullWritable, NullWritable> {
    
    @Override
    public void reduce(Text key, Iterable<From> values, Reducer<Text,From,NullWritable,NullWritable>.Context context) {
        Logger.getLogger(this.getClass()).info(key.toString() + ":" + values.toString());
    }
    
}
