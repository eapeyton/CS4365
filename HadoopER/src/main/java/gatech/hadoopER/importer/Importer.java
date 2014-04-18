package gatech.hadoopER.importer;

import gatech.hadoopER.ERJob;
import java.io.IOException;
import java.util.ArrayList;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

/**
 *
 * @author eric
 * @param <F>
 * @param <T>
 */
public abstract class Importer<F extends From,T extends To> implements ERJob {
    
    protected abstract void map(F from, T to);
    protected abstract void writableToFrom(Writable writable, F from);
    protected abstract Class<? extends InputFormat> getInputFormat();
    protected abstract Class<F> getFrom();
    protected abstract Class<T> getTo(); 
    
    public Job createJob(Configuration conf) throws IOException {
        Job job = Job.getInstance(conf);
        job.setJobName(this.getClass().getSimpleName() + "Importer");
        
        job.getConfiguration().setClass("Importer", this.getClass(), this.getClass());
        
        job.setInputFormatClass(getInputFormat()); 
        job.setMapperClass(ImporterMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(getFrom());
        
        job.setReducerClass(ImporterReducer.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(getTo());
        
        job.setJarByClass(this.getClass());
        job.setNumReduceTasks(1);
        return job;
    }
    
    protected static Importer getInstance(JobContext context) {
        try {
            Class<Importer> importerClass = (Class<Importer>)context.getConfiguration().getClass("Importer", null);
            return importerClass.newInstance();
        } catch (InstantiationException | IllegalAccessException ex) {
            Logger.getLogger(ImporterMapper.class.getName()).log(Level.SEVERE, null, ex);
            throw new RuntimeException(ex);
        }
    }
    
    protected T getFromTo(F from) {
        try {
            T to = getTo().newInstance();
            to.uuid = UUID.randomUUID();
            map(from, to);
            return to;
        } catch (InstantiationException | IllegalAccessException ex) {
            Logger.getLogger(Importer.class.getName()).log(Level.SEVERE, null, ex);
            throw new RuntimeException(ex);
        }
    }
        
    protected F getFromWritable(Writable writable) {
        try {
            F from = getFrom().newInstance();
            writableToFrom(writable, from);
            return from;
        } catch (InstantiationException | IllegalAccessException ex) {
            Logger.getLogger(Importer.class.getName()).log(Level.SEVERE, null, ex);
            throw new RuntimeException(ex);
        }
    }
     
}
