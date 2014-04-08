package gatech.hadoopdedoopmaven;


import java.io.IOException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
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
    protected abstract Class<F> getFrom();
    protected abstract Class<T> getTo(); 
    
    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance();
        job.setJobName(this.getClass().getCanonicalName() + "Importer");
        
        job.setMapperClass(ImporterMapper.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
        job.setReducerClass(ImporterReducer.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);
        
        job.setJarByClass(Importer.class);
        
        if(job.waitForCompletion(true)) {
            return 0;
        }
        return 1;
    }
    
    
    
    public static Importer getInstance(JobContext context) {
        try {
            Class<Importer> importerClass = (Class<Importer>)context.getConfiguration().getClass("Importer", null);
            return importerClass.newInstance();
        } catch (InstantiationException ex) {
            Logger.getLogger(ImporterMapper.class.getName()).log(Level.SEVERE, null, ex);
            throw new RuntimeException(ex);
        } catch (IllegalAccessException ex) {
            Logger.getLogger(ImporterMapper.class.getName()).log(Level.SEVERE, null, ex);
            throw new RuntimeException(ex);
        }
    }
    
    public To getFromTo(From from) {
        try {
            T to = getTo().newInstance();
            map((F)from, to);
            return to;
        } catch (InstantiationException ex) {
            Logger.getLogger(Importer.class.getName()).log(Level.SEVERE, null, ex);
            throw new RuntimeException(ex);
        } catch (IllegalAccessException ex) {
            Logger.getLogger(Importer.class.getName()).log(Level.SEVERE, null, ex);                    
            throw new RuntimeException(ex);
        }
    }
    
    public From getFromText(Text value) {
        try {
            From from = getFrom().newInstance();
            return from;
        } catch (InstantiationException ex) {
            Logger.getLogger(Importer.class.getName()).log(Level.SEVERE, null, ex);
            throw new RuntimeException(ex);
        } catch (IllegalAccessException ex) {
            Logger.getLogger(Importer.class.getName()).log(Level.SEVERE, null, ex);                    
            throw new RuntimeException(ex);
        }
    }
    
    public void runImport(String[] args) throws Exception {
        ToolRunner.run(this.getClass().newInstance(), args);
    }
        
    public void doImport(F[] inputs) throws InstantiationException, IllegalAccessException {
        for(F input: inputs) {
            T output = getTo().newInstance();
            map(input,output);
            outputs.add(output);
        }
    }
}
