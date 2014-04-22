/*
 * CS 4365 Project
 */

package gatech.hadoopER;

import gatech.hadoopER.builder.Builder;
import gatech.hadoopER.combiner.Combiner;
import gatech.hadoopER.events.EventRunner;
import gatech.hadoopER.importer.Importer;
import gatech.hadoopER.importer.To;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author eric
 */
public abstract class Runner<T extends To,A extends ArrayWritable> extends Configured implements Tool {
    
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
    private final Path COMBINER_OUTPUT = HOME.suffix("/combiner-output");
    private final Path EXPORTER_OUTPUT = HOME.suffix("/exporter-output");
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
        return 0;
    }
    
}
