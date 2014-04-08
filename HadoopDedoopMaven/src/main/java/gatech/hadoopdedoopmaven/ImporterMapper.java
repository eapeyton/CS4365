package gatech.hadoopdedoopmaven;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author eric
 */
public class ImporterMapper extends Mapper<LongWritable, Text, Text, From> {
    private Importer importer;
    
    @Override
    protected void setup(Context context) {
        importer = Importer.getInstance(context);
    }
    
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, From>.Context context) throws IOException, InterruptedException {
        From from = importer.getFromText(value);
        context.write(new Text(from.getKey()), from);
    }
}
