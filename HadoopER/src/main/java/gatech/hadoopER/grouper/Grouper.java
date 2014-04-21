/*
 * CS 4365 Project
 */

package gatech.hadoopER.grouper;

import gatech.hadoopER.importer.To;
import gatech.hadoopER.util.Util;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.log4j.Logger;

/**
 *
 * @author eric
 */
public class Grouper<T extends To> {
    private Configuration conf;
    private T key;
    private T value;
    
    public Grouper(Configuration conf, T key, T value) {
        this.conf = conf;
        this.key = key;
        this.value = value;
    }

    public void group(Path input, Path output) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        SequenceFile.Reader reader;
        for(Path item: Util.recurseDir(fs, input)) {
            reader = new SequenceFile.Reader(conf, Reader.file(item));
            while(reader.next(key, value)) {
                Logger.getLogger(Grouper.class).info("GKey:" + key.toString() + "\nGValue:" + value.toString());
            }
            reader.close();
        }
        

    }
}
