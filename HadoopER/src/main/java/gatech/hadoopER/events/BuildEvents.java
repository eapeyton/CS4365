/*
 * CS 4365 Project
 */
package gatech.hadoopER.events;

import gatech.hadoopER.builder.Builder;

/**
 *
 * @author eric
 */
public class BuildEvents extends Builder<GlobalEvent> {

    @Override
    protected boolean areMatching(GlobalEvent a, GlobalEvent b) {

        return true;
    }

}
