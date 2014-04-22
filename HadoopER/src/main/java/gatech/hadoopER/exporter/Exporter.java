/*
 * CS 4365 Project
 */

package gatech.hadoopER.exporter;

import gatech.hadoopER.ERJob;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 *
 * @author eric
 */
public class Exporter implements ERJob {
     
    @Override
    public Job createJob(Configuration conf) throws IOException {
        Job job = Job.getInstance(conf);
        job.setJobName(this.getClass().getSimpleName());
        
        job.getConfiguration().setClass("Exporter", this.getClass(), this.getClass());
        Class<?> toClass = conf.getClass("ToClass", null);

        job.setInputFormatClass(SequenceFileInputFormat.class); 
        job.setMapperClass(ExporterMapper.class);
        TextOutputFormat.setCompressOutput(job, false);
        conf.setBoolean("mapred.compress.map.output", false);
        conf.setBoolean("mapred.output.compress", false);
        
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        
        job.setJarByClass(this.getClass());
        job.setNumReduceTasks(0);
        return job;
    }   
}
