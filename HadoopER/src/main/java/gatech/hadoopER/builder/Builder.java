/*
 * CS 4365 Project
 */
package gatech.hadoopER.builder;

import gatech.hadoopER.ERJob;
import gatech.hadoopER.global.To;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

/**
 *
 * @author eric
 */
public abstract class Builder<T extends To> implements ERJob {

    protected abstract boolean areMatching(T a, T b);

    @Override
    public Job createJob(Configuration conf) throws IOException {
        Job job = Job.getInstance(conf);
        job.setJobName(this.getClass().getSimpleName() + " Builder");

        job.getConfiguration().setClass("Builder", this.getClass(), this.getClass());
        Class<?> toClass = conf.getClass("ToClass", null);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setMapperClass(BuilderMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(toClass);

        job.setReducerClass(BuilderReducer.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputKeyClass(toClass);
        job.setOutputValueClass(toClass);

        job.setJarByClass(this.getClass());
        job.setNumReduceTasks(conf.getInt("NumReduceTasks", 1));
        return job;
    }

    protected static Builder getInstance(JobContext context) {
        try {
            Class<Builder> builderClass = (Class<Builder>) context.getConfiguration().getClass("Builder", null);
            return builderClass.newInstance();
        } catch (InstantiationException | IllegalAccessException ex) {
            java.util.logging.Logger.getLogger(Builder.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
            throw new RuntimeException(ex);
        }
    }

}
