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
import gatech.hadoopER.util.Util;
import java.io.IOException;
import java.util.List;
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
        fs = FileSystem.get(conf);
        runImport(conf);
        return 0;
    }

    public void runImport(Configuration conf) throws IOException, InterruptedException, ClassNotFoundException {
        fs.delete(IMPORTER_OUTPUT, true);
        fs.mkdirs(IMPORTER_OUTPUT);
        for (Importer importer : getImporters()) {
            Job importerJob = importer.createJob(conf);
            FileInputFormat.setInputPaths(importerJob, importer.getInputPath());
            FileOutputFormat.setOutputPath(importerJob, IMPORTER_OUTPUT.suffix("/" + importer.getClass().getSimpleName() + "/"));
            importerJob.waitForCompletion(true);
        }

    }

    public void runBuilder(Configuration conf) throws IOException, InterruptedException, ClassNotFoundException, InstantiationException, IllegalAccessException {
        Job builder = getBuilder().createJob(conf);
        for (Path item : Util.recurseDir(fs, IMPORTER_OUTPUT)) {
            FileInputFormat.addInputPath(builder, item);
        }

        fs.delete(BUILDER_OUTPUT, true);

        FileOutputFormat.setOutputPath(builder, BUILDER_OUTPUT);

        builder.waitForCompletion(true);

        fs.delete(GROUPER_OUTPUT, true);
        fs.mkdirs(GROUPER_OUTPUT);
        Grouper<T, A> grouper = new Grouper<>(conf, getToClass().newInstance(), getToClass().newInstance());
        grouper.group(BUILDER_OUTPUT, GROUPER_OUTPUT.suffix("/groups.seq"));

    }

    public void runCombiner(Configuration conf) throws IOException, InterruptedException, ClassNotFoundException {
        Job combiner = getCombiner().createJob(conf);
        FileInputFormat.addInputPath(combiner, GROUPER_OUTPUT);

        fs.delete(COMBINER_OUTPUT, true);

        FileOutputFormat.setOutputPath(combiner, COMBINER_OUTPUT);

        combiner.waitForCompletion(true);
    }

    public void runExport(Configuration conf) throws IOException, InterruptedException, ClassNotFoundException {
        Job exporter = new Exporter().createJob(conf);
        FileInputFormat.addInputPath(exporter, COMBINER_OUTPUT);

        fs.delete(EXPORTER_OUTPUT, true);

        FileOutputFormat.setOutputPath(exporter, EXPORTER_OUTPUT);

        exporter.waitForCompletion(true);

    }

}
