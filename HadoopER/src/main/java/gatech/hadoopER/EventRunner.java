/*
 * CS 4365 Project
 */

package gatech.hadoopER;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author eric
 */
public class EventRunner extends Configured implements Tool {
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int res;
        res = ToolRunner.run(conf, new EventRunner(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = super.getConf();
        Job importA = new ImportEventsA().createJob(conf);
        Job importB = new ImportEventsB().createJob(conf);
        
        
        FileInputFormat.setInputPaths(importA, new Path("/user/epeyton.site/a-source/compact-events.json"));
        FileInputFormat.setInputPaths(importB, new Path("/user/epeyton.site/b-source/events2.xml"));
        
        Path output = new Path("/user/epeyton.site/importer-output/");
        FileSystem fs = FileSystem.get(conf);
        fs.delete(output, true);
        fs.mkdirs(output);
        
        FileOutputFormat.setOutputPath(importA, output.suffix("/source-a/"));
        FileOutputFormat.setOutputPath(importB, output.suffix("/source-b/")); 
        
        if(!importA.waitForCompletion(true)) {
            return 1;
        }
        if(!importB.waitForCompletion(true)) {
            return 1;
        }
        return 0;
    }
}
