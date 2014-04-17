/*
 * CS 4365 Project
 */

package gatech.hadoopER;

import gatech.hadoopER.importer.ImporterXml;

/**
 *
 * @author eric
 */
public class ImportEventsB extends ImporterXml<SampleEventB, GlobalEvent>{
    
    public static void main(String[] args) throws Exception {
        new ImportEventsB().go(args);
    }

    @Override
    protected void map(SampleEventB from, GlobalEvent to) {
        to.title = from.name;
        to.other = from.description;
        to.location = from.latitude + "," + from.longitude;
        to.datetime = from.start_time;
    }

    @Override
    protected Class<SampleEventB> getFrom() {
        return SampleEventB.class;
    }

    @Override
    protected Class<GlobalEvent> getTo() {
        return GlobalEvent.class;
    }
    
}
