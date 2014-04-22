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
import org.apache.hadoop.io.ArrayWritable;
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
    private static final Path COMBINER_OUTPUT = HOME.suffix("/combiner-output");
    private FileSystem fs;

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = super.getConf();
        conf.setClass("ToClass", GlobalEvent.class, GlobalEvent.class);
        conf.setClass("ToArrayClass", GEArrayWritable.class, GEArrayWritable.class);
        fs = FileSystem.get(conf);

        runImport(conf);
        runBuilder(conf);
        runCombiner(conf);

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
    
    public void runBuilder(Configuration conf) throws IOException, InterruptedException, ClassNotFoundException, InstantiationException, IllegalAccessException {
        Job builder = new BuildEvents().createJob(conf);
        for(Path item: Util.recurseDir(fs, IMPORTER_OUTPUT)) {
            FileInputFormat.addInputPath(builder, item);
        }
        
        fs.delete(BUILDER_OUTPUT, true);
        
        FileOutputFormat.setOutputPath(builder, BUILDER_OUTPUT);
        
        builder.waitForCompletion(true);
        
        fs.delete(GROUPER_OUTPUT, true);
        fs.mkdirs(GROUPER_OUTPUT);
        Grouper<GlobalEvent,GEArrayWritable> grouper = new Grouper<>(conf, new GlobalEvent(), new GlobalEvent());
        grouper.group(BUILDER_OUTPUT, GROUPER_OUTPUT.suffix("/groups.seq"));
        
    }

    public void runCombiner(Configuration conf) throws IOException, InterruptedException, ClassNotFoundException {
        Job combiner = new CombineEvents().createJob(conf);
        FileInputFormat.addInputPath(combiner, GROUPER_OUTPUT);
        
        fs.delete(COMBINER_OUTPUT, true);
        
        FileOutputFormat.setOutputPath(combiner, COMBINER_OUTPUT);
        
        combiner.waitForCompletion(true);
    }
    
}
