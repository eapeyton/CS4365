package gatech.hadoopdedoopmaven;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author eric
 */
public class ImporterReducer extends Reducer<Text, Text, NullWritable, NullWritable> {
    
}
