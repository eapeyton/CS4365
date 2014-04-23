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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.mapreduce.Counter;
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

    private LinkedHashMap<String, String> stats = new LinkedHashMap<>();
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
        boolean first_run = true;
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("stats.csv"), "utf-8"))) {
            int[] reducer_values = {20, 50, 100, 200};
            for (int reducer_value : reducer_values) {
                Configuration conf = super.getConf();
                conf.setClass("ToClass", getToClass(), getToClass());
                conf.setClass("ToArrayClass", getToArrayClass(), getToArrayClass());
                conf.setInt("NumReduceTasks", reducer_value);
                stats.put("Num. Reducers", Integer.toString(conf.getInt("NumReduceTasks", -1)));
                fs = FileSystem.get(conf);
                runImport(conf);
                runBuilder(conf);
                runCombiner(conf);
                runExporter(conf);
                if(first_run) {
                    writer.write(StringUtils.join(stats.keySet(), ","));
                    first_run = false;
                }
                writer.newLine();
                writer.write(StringUtils.join(stats.values(), ","));
            }
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
            records += importerJob.getCounters().getGroup("org.apache.hadoop.mapreduce.TaskCounter").findCounter("MAP_INPUT_RECORDS").getValue();
        }
        stats.put("Importer Total Time", Long.toString(timer.end()));
        stats.put("Importer Input Records", Long.toString(records));
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
        writeStats("Builder", builder, stats);

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
        writeStats("Combiner", combiner, stats);
    }

    public void runExporter(Configuration conf) throws IOException, InterruptedException, ClassNotFoundException {
        Job exporter = new Exporter().createJob(conf);
        FileInputFormat.addInputPath(exporter, COMBINER_OUTPUT);

        fs.delete(EXPORTER_OUTPUT, true);

        FileOutputFormat.setOutputPath(exporter, EXPORTER_OUTPUT);
        timer.start();
        exporter.waitForCompletion(true);
        stats.put("Exporter Time", Long.toString(timer.end()));
        writeStats("Exporter", exporter, stats);
    }

    private void writeStats(String jobName, Job job, Map<String, String> stats) throws IOException {
        String group_task = "org.apache.hadoop.mapreduce.TaskCounter";
        String[] task_counters = {
            "MAP_INPUT_RECORDS",
            "REDUCE_INPUT_GROUPS",
            "REDUCE_INPUT_RECORDS"
        };
        String group_job = "org.apache.hadoop.mapreduce.JobCounter";
        String[] job_counters = {
            "MILLIS_MAPS",
            "MILLIS_REDUCES"
        };
        for (String counterName : task_counters) {
            Counter counter = job.getCounters().getGroup(group_task).findCounter(counterName);
            stats.put(jobName + " " + counter.getDisplayName(), Long.toString(counter.getValue()));
        }
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
