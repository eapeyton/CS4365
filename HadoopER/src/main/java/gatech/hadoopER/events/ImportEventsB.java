/*
 * CS 4365 Project
 */

package gatech.hadoopER.events;

import gatech.hadoopER.importer.ImporterXml;

/**
 *
 * @author eric
 */
public class ImportEventsB extends ImporterXml<SampleEventB, GlobalEvent>{
    
    @Override
    protected void map(SampleEventB from, GlobalEvent to) {
        to.title = from.name;
        to.other = from.description;
        if(from.latitude != null & from.longitude != null) {
            to.location = from.latitude + "," + from.longitude;
        }
        else {
            to.location = from.location;
        }
        to.datetime = from.start_time;
    }

    @Override
    public String getTagName() {
        return "event";
    }

    @Override
    protected Class<SampleEventB> getFromClass() {
        return SampleEventB.class;
    }
    

    
}
