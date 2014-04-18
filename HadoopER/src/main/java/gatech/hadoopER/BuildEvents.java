/*
 * CS 4365 Project
 */

package gatech.hadoopER;

import gatech.hadoopER.builder.Builder;

/**
 *
 * @author eric
 */
public class BuildEvents extends Builder<GlobalEvent> {

    @Override
    protected Class<GlobalEvent> getTo() {
        return GlobalEvent.class;
    }
    
}
