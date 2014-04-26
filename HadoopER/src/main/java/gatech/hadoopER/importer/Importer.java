package gatech.hadoopER.importer;

import gatech.hadoopER.global.To;
import gatech.hadoopER.ERJob;
import java.io.IOException;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

/**
 *
 * @author eric
 * @param <F>
 * @param <T>
 */
public abstract class Importer<F extends From, T extends To> implements ERJob {

    private Class<F> fromClass;
    private Class<T> toClass;

    /**
     * Return the class that represents the source schema
     * @return the class that represents the source schema
     */
    protected abstract Class<F> getFromClass();

    /**
     * Map the input schema (From) to the global schema (To).
     * Use the attributes in the "from" object to fill in attributes in the "to" object.
     * @param from the import record
     * @param to the global record
     */
    protected abstract void map(F from, T to);

    /**
     * Produce an import record from a writable input.
     * @param writable the writable from input to the map-reduce job
     * @param from an object that represents the import schema
     */
    protected abstract void writableToFrom(Writable writable, F from);

    /**
     * Return the input format of this importer.
     * @return the input format
     */
    protected abstract Class<? extends InputFormat> getInputFormat();

    /**
     * Return the input path for the data to import in HDFS
     * @return the input path
     */
    public abstract Path getInputPath();

    public Job createJob(Configuration conf) throws IOException {
        Job job = Job.getInstance(conf);
        job.setJobName(this.getClass().getSimpleName() + "Importer");

        job.getConfiguration().setClass("Importer", this.getClass(), this.getClass());
        job.getConfiguration().setClass("FromClass", getFromClass(), getFromClass());

        job.setInputFormatClass(getInputFormat());
        job.setMapperClass(ImporterMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(getFromClass());

        job.setReducerClass(ImporterReducer.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(conf.getClass("ToClass", null));

        job.setJarByClass(this.getClass());
        job.setNumReduceTasks(conf.getInt("NumReduceTasks", 1));
        return job;
    }

    protected static Importer getInstance(JobContext context) {
        try {
            Class<Importer> importerClass = (Class<Importer>) context.getConfiguration().getClass("Importer", null);
            Importer importer = importerClass.newInstance();
            importer.fromClass = context.getConfiguration().getClass("FromClass", null);
            importer.toClass = context.getConfiguration().getClass("ToClass", null);
            return importer;
        } catch (InstantiationException | IllegalAccessException ex) {
            Logger.getLogger(ImporterMapper.class.getName()).log(Level.SEVERE, null, ex);
            throw new RuntimeException(ex);
        }
    }

    protected T getFromTo(F from) {
        try {
            T to = toClass.newInstance();
            to.uuid = UUID.randomUUID();
            map(from, to);
            return to;
        } catch (InstantiationException | IllegalAccessException ex) {
            Logger.getLogger(Importer.class.getName()).log(Level.SEVERE, null, ex);
            throw new RuntimeException(ex);
        }
    }

    protected F getFromWritable(Writable writable) {
        try {
            F from = fromClass.newInstance();
            writableToFrom(writable, from);
            return from;
        } catch (InstantiationException | IllegalAccessException ex) {
            Logger.getLogger(Importer.class.getName()).log(Level.SEVERE, null, ex);
            throw new RuntimeException(ex);
        }
    }

}
