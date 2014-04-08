package gatech.hadoopdedoopmaven;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author eric
 */
public class ImporterMapper extends Mapper<LongWritable, Text, Text, Text> {
    
}
