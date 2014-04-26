/*
 * CS 4365 Project
 */
package gatech.hadoopER.exporter;

import com.google.gson.Gson;
import gatech.hadoopER.global.To;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author eric
 */
public class ExporterMapper extends Mapper<IntWritable, To, NullWritable, Text> {

    private Gson gson;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.gson = new Gson();
    }

    @Override
    protected void map(IntWritable key, To value, Context context) throws IOException, InterruptedException {
        String json = gson.toJson(value);
        context.write(NullWritable.get(), new Text(json));
    }

}
