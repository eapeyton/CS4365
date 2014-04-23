/*
 * CS 4365 Project
 */
package gatech.hadoopER.builder;

import gatech.hadoopER.importer.To;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

/**
 *
 * @author eric
 */
public class BuilderReducer extends Reducer<Text, To, To, To> {

    private Builder builder;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        builder = Builder.getInstance(context);
    }

    @Override
    protected void reduce(Text key, Iterable<To> values, Context context) throws IOException, InterruptedException {
        int i = 0;

        Logger.getLogger(this.getClass()).info("Key: " + key.toString());
        List<To> cloned = new ArrayList<>();
        for (To value : values) {
            cloned.add(WritableUtils.clone(value, context.getConfiguration()));
        }
        if (key.toString().equals("Base-Case")) {
            for (To value : cloned) {
                context.write(value, value);
            }
        } else {
            for (To value : cloned) {
                //Logger.getLogger(this.getClass()).info("Value: " + value.toString());
                int j = 0;
                for (To otherValue : cloned) {
                    if (j > i) {
                        if (builder.areMatching(value, otherValue)) {
                            context.write(value, otherValue);
                        }
                    }
                    j++;
                }
                i++;
            }
        }

    }

}
