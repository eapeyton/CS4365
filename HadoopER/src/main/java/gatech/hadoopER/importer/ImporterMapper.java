package gatech.hadoopER.importer;

import gatech.hadoopER.global.To;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

/**
 *
 * @author eric
 */
public class ImporterMapper extends Mapper<LongWritable, Writable, Text, From> {

    private Importer<From, To> importer;

    @Override
    protected void setup(Context context) {
        importer = Importer.getInstance(context);
    }

    @Override
    protected void map(LongWritable key, Writable value, Mapper<LongWritable, Writable, Text, From>.Context context) throws IOException, InterruptedException {
        From from = importer.getFromWritable(value);
        Logger.getLogger(this.getClass()).info("from:" + from.getKey());
        context.write(new Text(from.getKey()), from);
    }
}
