/*
 * CS 4365 Project
 */
package gatech.hadoopER.grouper;

import gatech.hadoopER.importer.To;
import gatech.hadoopER.util.Util;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
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

    public void group(Path input, Path output, Class<T> clazz) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        SequenceFile.Reader reader;
        Map<T, Set<T>> map = new HashMap<>();
        for (Path item : Util.recurseDir(fs, input)) {
            reader = new SequenceFile.Reader(conf, Reader.file(item));
            while (reader.next(key, value)) {
                T clonedKey = WritableUtils.clone(key, conf);
                T clonedValue = WritableUtils.clone(value, conf);
                if (!map.containsKey(clonedKey) && !map.containsKey(clonedValue)) {
                    Set<T> set = new HashSet<>();
                    set.add(clonedKey);
                    set.add(clonedValue);
                    map.put(clonedKey, set);
                    map.put(clonedValue, set);
                } else if (map.containsKey(clonedKey) && map.containsKey(clonedValue)) {
                    for (T clone : map.get(clonedValue)) {
                        map.put(clone, map.get(clonedKey));
                    }
                    map.get(clonedKey).addAll(map.get(clonedValue));
                } else if (map.containsKey(clonedKey)) {
                    map.get(clonedKey).add(clonedValue);
                    map.put(clonedValue, map.get(clonedKey));
                } else {
                    map.get(clonedValue).add(clonedKey);
                    map.put(clonedKey, map.get(clonedValue));
                }
            }
            reader.close();
        }
                Logger.getLogger(this.getClass()).info("ValuesSize: " + map.values().size());

        Set<Set<T>> deduped = new HashSet<>();
        for(Set<T> set: map.values()) {
            deduped.add(set);
        }
        Logger.getLogger(this.getClass()).info("DDSize: " + deduped.size());
        SequenceFile.Writer writer = SequenceFile.createWriter(conf, Writer.file(output), Writer.keyClass(IntWritable.class), Writer.valueClass(ArrayWritable.class));
        int i=0;
        for(Set<T> set: deduped) {
            T[] arr = (T[])new Object[set.size()];
            set.toArray(arr);
            Logger.getLogger(this.getClass()).info(Arrays.toString(arr));

            ArrayWritable arrW = new ArrayWritable(clazz);
            arrW.set(arr);
            writer.append(new IntWritable(i), arrW);
            i++;
        }
        writer.close();
        
        

    }
}
