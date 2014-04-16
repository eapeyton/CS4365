package gatech.hadoopdedoopmaven;

import java.io.IOException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author eric
 * @param <F>
 * @param <T>
 */
public abstract class Importer<F extends From,T extends To> extends Configured implements Tool {
    
    private final ArrayList<T> outputs = new ArrayList<>();
    protected abstract void map(F from, T to);
    protected abstract void writableToFrom(Writable writable, F from);
    protected abstract Class<F> getFrom();
    protected abstract Class<T> getTo(); 
    
    @Override
    public int run(String[] args) throws Exception {
        Job job = createJob();
        FileInputFormat.setInputPaths(job, args[0]);

        if(job.waitForCompletion(true)) {
            return 0;
        }
        return 1;
    }
    
    protected Job createJob() throws IOException {
        Job job = Job.getInstance(super.getConf());
        job.setJobName(this.getClass().getSimpleName() + "Importer");
        
        job.getConfiguration().setClass("Importer", this.getClass(), this.getClass());
        
        job.setMapperClass(ImporterMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(From.class);
        
        job.setReducerClass(ImporterReducer.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);
        
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
    
    protected void runImport(String[] args) throws Exception {
        ToolRunner.run(this.getClass().newInstance(), args);
    }
        
    protected void doImport(F[] inputs) throws InstantiationException, IllegalAccessException {
        for(F input: inputs) {
            T output = getTo().newInstance();
            map(input,output);
            outputs.add(output);
        }
    }
}
