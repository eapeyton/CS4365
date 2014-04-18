/*
 * CS 4365 Project
 */

package gatech.hadoopER.builder;

import gatech.hadoopER.importer.To;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author eric
 */
public class BuilderReducer extends Reducer<Text,To,Text,To> {

    @Override
    protected void reduce(Text key, Iterable<To> values, Context context) throws IOException, InterruptedException {
        super.reduce(key, values, context); //To change body of generated methods, choose Tools | Templates.
    }
    
}
