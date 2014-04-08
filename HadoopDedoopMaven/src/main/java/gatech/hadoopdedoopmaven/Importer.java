package gatech.hadoopdedoopmaven;


import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author eric
 * @param <T>
 * @param <K>
 */
public abstract class Importer<T extends From,K extends To> extends Configured implements Tool {
    
    private final ArrayList<K> outputs = new ArrayList<>();
    protected abstract void map(T from, K to);
    protected abstract Class<T> getFrom();
    protected abstract Class<K> getTo(); 
    
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
    
    public void runImport(String[] args) throws Exception {
        ToolRunner.run(this.getClass().newInstance(), args);
    }
        
    public void doImport(T[] inputs) throws InstantiationException, IllegalAccessException {
        for(T input: inputs) {
            K output = getTo().newInstance();
            map(input,output);
            outputs.add(output);
        }
    }
}
