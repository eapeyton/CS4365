/*
 * CS 4365 Project
 */
package gatech.hadoopER.combiner;

import gatech.hadoopER.importer.To;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

/**
 *
 * @author eric
 */
public class CombinerMapper extends Mapper<IntWritable, ArrayWritable, IntWritable, To> {

    private Combiner combiner;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        combiner = Combiner.getInstance(context);
    }

    @Override
    protected void map(IntWritable key, ArrayWritable value, Context context) throws IOException, InterruptedException {
        Writable[] group = value.get();
        Logger.getLogger(this.getClass()).info("Combiner found:" + Arrays.toString(group));
        List<Writable> groupList = Arrays.asList(group);
        To combined = combiner.combine(groupList);
        context.write(key, combined);
    }

}
