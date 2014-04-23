/*
 * CS 4365 Project
 */
package gatech.hadoopER;

import gatech.hadoopER.builder.Builder;
import gatech.hadoopER.combiner.Combiner;
import gatech.hadoopER.exporter.Exporter;
import gatech.hadoopER.grouper.Grouper;
import gatech.hadoopER.importer.Importer;
import gatech.hadoopER.importer.To;
import gatech.hadoopER.util.ERUtil;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author eric
 */
public abstract class Runner<T extends To, A extends ArrayWritable> extends Configured implements Tool {

    public abstract Class<T> getToClass();

    public abstract Class<A> getToArrayClass();

    public abstract List<Importer> getImporters();

    public abstract Builder<T> getBuilder();

    public abstract Combiner<T> getCombiner();

    public abstract Path getHome();
    
    private LinkedHashMap<String,String> stats = new LinkedHashMap<>();
    private Timer timer = new Timer();
    private final Path HOME = getHome();
    private final Path IMPORTER_OUTPUT = HOME.suffix("/importer-output/");
    private final Path BUILDER_OUTPUT = HOME.suffix("/builder-output/");
    private final Path GROUPER_OUTPUT = HOME.suffix("/grouper-output/");
    private final Path COMBINER_OUTPUT = HOME.suffix("/combiner-output/");
    private final Path EXPORTER_OUTPUT = HOME.suffix("/exporter-output/");
    private FileSystem fs;

    public static void start(Runner self, String[] args) throws Exception {
        Configuration conf = new Configuration();
        int res;
        res = ToolRunner.run(conf, self, args);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = super.getConf();
        conf.setClass("ToClass", getToClass(), getToClass());
        conf.setClass("ToArrayClass", getToArrayClass(), getToArrayClass());
        conf.setInt("NumReduceTasks", 50);
        stats.put("Num. Reducers", Integer.toString(conf.getInt("NumReduceTasks",-1)));
        fs = FileSystem.get(conf);
        runImport(conf);
        runBuilder(conf);
        runCombiner(conf);
        runExporter(conf);
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("stats.csv"),"utf-8"))) {
            writer.write(StringUtils.join(stats.keySet(), ","));
            writer.newLine();
            writer.write(StringUtils.join(stats.values(),","));
        }
        return 0;
    }

    public void runImport(Configuration conf) throws IOException, InterruptedException, ClassNotFoundException {
        fs.delete(IMPORTER_OUTPUT, true);
        fs.mkdirs(IMPORTER_OUTPUT);
        long records = 0;
        long bytes = 0;
        timer.start();
        for (Importer importer : getImporters()) {   
            Job importerJob = importer.createJob(conf);
            FileInputFormat.setInputPaths(importerJob, importer.getInputPath());
            FileOutputFormat.setOutputPath(importerJob, IMPORTER_OUTPUT.suffix("/" + importer.getClass().getSimpleName() + "/"));
            importerJob.waitForCompletion(true);
            records += importerJob.getCounters().getGroup("Map-Reduce Framework").findCounter("Map input records").getValue();
                        bytes += importerJob.getCounters().getGroup("Map-Reduce Framework").findCounter("Map input bytes").getValue();

        }
        stats.put("Importer Total Time", Long.toString(timer.end()));
        stats.put("Importer Input Records", Long.toString(records));
        stats.put("Importer Input Bytes", Long.toString(bytes));
    }

    public void runBuilder(Configuration conf) throws IOException, InterruptedException, ClassNotFoundException, InstantiationException, IllegalAccessException {
        Job builder = getBuilder().createJob(conf);
        for (Path item : ERUtil.recurseDir(fs, IMPORTER_OUTPUT)) {
            FileInputFormat.addInputPath(builder, item);
        }

        fs.delete(BUILDER_OUTPUT, true);

        FileOutputFormat.setOutputPath(builder, BUILDER_OUTPUT);
        
        timer.start();
        builder.waitForCompletion(true);
        stats.put("Builder Time", Long.toString(timer.end()));
        
        
        fs.delete(GROUPER_OUTPUT, true);
        fs.mkdirs(GROUPER_OUTPUT);
        Grouper<T, A> grouper = new Grouper<>(conf, getToClass().newInstance(), getToClass().newInstance());
        timer.start();
        grouper.group(BUILDER_OUTPUT, GROUPER_OUTPUT.suffix("/groups.seq"));
        stats.put("Grouper Time", Long.toString(timer.end()));
    }

    public void runCombiner(Configuration conf) throws IOException, InterruptedException, ClassNotFoundException {
        Job combiner = getCombiner().createJob(conf);
        FileInputFormat.addInputPath(combiner, GROUPER_OUTPUT);

        fs.delete(COMBINER_OUTPUT, true);

        FileOutputFormat.setOutputPath(combiner, COMBINER_OUTPUT);
        timer.start();
        combiner.waitForCompletion(true);
        stats.put("Combiner Time", Long.toString(timer.end()));
    }

    public void runExporter(Configuration conf) throws IOException, InterruptedException, ClassNotFoundException {
        Job exporter = new Exporter().createJob(conf);
        FileInputFormat.addInputPath(exporter, COMBINER_OUTPUT);

        fs.delete(EXPORTER_OUTPUT, true);

        FileOutputFormat.setOutputPath(exporter, EXPORTER_OUTPUT);
        timer.start();
        exporter.waitForCompletion(true);
        stats.put("Exporter Time", Long.toString(timer.end()));
    }
    
    public class Timer {
        private long startTime;
        
        public void start() {
            startTime = System.currentTimeMillis();
        }
        
        public long end() {
            long duration = System.currentTimeMillis() - startTime;
            return TimeUnit.MILLISECONDS.toSeconds(duration);
        }
    }

}
