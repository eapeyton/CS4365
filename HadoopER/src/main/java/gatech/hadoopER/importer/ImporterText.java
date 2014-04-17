package gatech.hadoopER.importer;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

/**
 *
 * @author eric
 */
public abstract class ImporterText<F extends From,T extends To> extends Importer<F,T> {

    @Override
    protected Job createJob() throws IOException {
        Job job = super.createJob();
        job.setInputFormatClass(TextInputFormat.class);
        return job;
    }
    
    @Override
    protected void writableToFrom(Writable writable, F from) {
        Text text = (Text)writable;
        textToFrom(text,from);
    }

    protected abstract void textToFrom(Text text, F from);
    
}
