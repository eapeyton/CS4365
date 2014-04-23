/*
 * CS 4365 Project
 */
package gatech.hadoopER.combiner;

import gatech.hadoopER.ERJob;
import gatech.hadoopER.importer.To;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

/**
 *
 * @author eric
 */
public abstract class Combiner<T extends To> implements ERJob {

    public abstract T combine(List<T> entities);

    @Override
    public Job createJob(Configuration conf) throws IOException {
        Job job = Job.getInstance(conf);
        job.setJobName(this.getClass().getSimpleName() + " Combiner");

        job.getConfiguration().setClass("Combiner", this.getClass(), this.getClass());
        Class<?> toClass = conf.getClass("ToClass", null);
        Class<?> toArrayClass = conf.getClass("ToArrayClass", null);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setMapperClass(CombinerMapper.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(toClass);;
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        //job.setReducerClass(Reducer.class);

        job.setJarByClass(this.getClass());
        job.setNumReduceTasks(conf.getInt("NumReduceTasks", 1));
        return job;
    }

    protected static Combiner getInstance(JobContext context) {
        try {
            Class<Combiner> builderClass = (Class<Combiner>) context.getConfiguration().getClass("Combiner", null);
            return builderClass.newInstance();
        } catch (InstantiationException | IllegalAccessException ex) {
            java.util.logging.Logger.getLogger(Combiner.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
            throw new RuntimeException(ex);
        }
    }

}
