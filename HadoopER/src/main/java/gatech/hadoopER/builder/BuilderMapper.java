/*
 * CS 4365 Project
 */

package gatech.hadoopER.builder;

import gatech.hadoopER.importer.To;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

/**
 *
 * @author eric
 */
public class BuilderMapper extends Mapper<Text,To,Text,To> {

    @Override
    protected void map(Text key, To value, Context context) throws IOException, InterruptedException {
        for(String blockKey: value.getBlockingKeys()) {
            context.write(new Text(blockKey.toLowerCase()), value);
        }
        Logger.getLogger(BuilderMapper.class).info(key.toString() + ":" + value.toString());
    }
    
    
    
}
