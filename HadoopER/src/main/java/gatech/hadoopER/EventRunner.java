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
    private static final Path IMPORTER_OUTPUT = HOME.suffix("/importer-output/");
    private static final Path BUILDER_OUTPUT = HOME.suffix("/builder-output/");
    private static final Path GROUPER_OUTPUT = HOME.suffix("/grouper-output/");
    private FileSystem fs;

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = super.getConf();
        fs = FileSystem.get(conf);

        runBuilder(conf);

        return 0;
    }
    
    public void runImport(Configuration conf) throws IOException, InterruptedException, ClassNotFoundException {
        fs.delete(IMPORTER_OUTPUT, true);
        fs.mkdirs(IMPORTER_OUTPUT);
        runImportA(conf);
        runImportB(conf);
    }
    
    public void runImportA(Configuration conf) throws IOException, InterruptedException, ClassNotFoundException {

        Job importA = new ImportEventsA().createJob(conf);
        FileInputFormat.setInputPaths(importA, HOME.suffix("/a-source/compact-events.json"));
        FileOutputFormat.setOutputPath(importA, IMPORTER_OUTPUT.suffix("/source-a/"));
        importA.waitForCompletion(true);
    }
    
    public void runImportB(Configuration conf) throws IOException, InterruptedException, ClassNotFoundException {
        
        Job importB = new ImportEventsB().createJob(conf);
        FileInputFormat.setInputPaths(importB, HOME.suffix("/b-source/events2.xml"));
        FileOutputFormat.setOutputPath(importB, IMPORTER_OUTPUT.suffix("/source-b/")); 
        importB.waitForCompletion(true);
    }
    
    public void runBuilder(Configuration conf) throws IOException, InterruptedException, ClassNotFoundException {
        Job builder = new BuildEvents().createJob(conf);
        for(Path item: Util.recurseDir(fs, IMPORTER_OUTPUT)) {
            FileInputFormat.addInputPath(builder, item);
        }
        
        fs.delete(BUILDER_OUTPUT, true);
        
        FileOutputFormat.setOutputPath(builder, BUILDER_OUTPUT);
        
        builder.waitForCompletion(true);
        
        fs.delete(GROUPER_OUTPUT, true);
        Grouper<GlobalEvent> grouper = new Grouper<>(conf, new GlobalEvent(), new GlobalEvent());
        grouper.group(BUILDER_OUTPUT, GROUPER_OUTPUT);
        
    }
}
