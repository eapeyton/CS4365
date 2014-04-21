/*
 * CS 4365 Project
 */

package gatech.hadoopER;

import gatech.hadoopER.grouper.Grouper;
import gatech.hadoopER.util.Util;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

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
    
    private static final Path HOME = new Path("/user/epeyton.site/");
    private static final Path IMPORTER_OUTPUT = HOME.suffix("importer-output/");
    private static final Path BUILDER_OUTPUT = HOME.suffix("builder-output/");
    private static final Path GROUPER_OUTPUT = HOME.suffix("grouper-output/");;

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = super.getConf();
        Job importA = new ImportEventsA().createJob(conf);
        Job importB = new ImportEventsB().createJob(conf);
        Job builder = new BuildEvents().createJob(conf);
        
        
        FileInputFormat.setInputPaths(importA, HOME.suffix("a-source/compact-events.json"));
        FileInputFormat.setInputPaths(importB, HOME.suffix("b-source/events2.xml"));
        
        FileSystem fs = FileSystem.get(conf);
        fs.delete(IMPORTER_OUTPUT, true);
        fs.mkdirs(IMPORTER_OUTPUT);
        
        FileOutputFormat.setOutputPath(importA, IMPORTER_OUTPUT.suffix("source-a/"));
        FileOutputFormat.setOutputPath(importB, IMPORTER_OUTPUT.suffix("source-b/")); ;
        
        if(!importA.waitForCompletion(true)) {
            return 1;
        }
        if(!importB.waitForCompletion(true)) {
            return 1;
        }
        
        for(Path item: Util.recurseDir(fs, IMPORTER_OUTPUT)) {
            FileInputFormat.addInputPath(builder, item);
        }
        
        fs.delete(BUILDER_OUTPUT, true);
        
        FileOutputFormat.setOutputPath(builder, BUILDER_OUTPUT);
        
        if(!builder.waitForCompletion(true)) {
            return 1;
        }
        
        fs.delete(GROUPER_OUTPUT, true);
        Grouper<GlobalEvent> grouper = new Grouper<>(conf, new GlobalEvent(), new GlobalEvent());
        grouper.group(BUILDER_OUTPUT, GROUPER_OUTPUT);
        
        
        return 0;
    }
}
