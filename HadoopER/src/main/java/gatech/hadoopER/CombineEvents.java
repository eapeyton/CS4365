/*
 * CS 4365 Project
 */

package gatech.hadoopER;

import gatech.hadoopER.combiner.Combiner;
import java.util.List;

/**
 *
 * @author eric
 */
public class CombineEvents extends Combiner<GlobalEvent> {

    @Override
    public GlobalEvent combine(List<GlobalEvent> entities) {
        return entities.get(0);
    }
    
}
