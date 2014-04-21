/*
 * CS 4365 Project
 */

package gatech.hadoopER.builder;

import gatech.hadoopER.importer.To;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

/**
 *
 * @author eric
 */
public class BuilderReducer extends Reducer<Text,To,To,To> {
    
    private Builder builder;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        builder = Builder.getInstance(context);
    }

    @Override
    protected void reduce(Text key, Iterable<To> values, Context context) throws IOException, InterruptedException {
        int i=0;
        Logger.getLogger(this.getClass()).info("Key: " + key.toString());
        for(To value: values) {
            Logger.getLogger(this.getClass()).info("Value: " + value.toString());
            int j=0;
            for(To otherValue: values) {
                if(j > i) {
                    if(builder.areMatching(value, otherValue)) {
                        context.write(value, otherValue);
                    }
                }
                j++;
            }
            i++;
        }
    }
    
}
