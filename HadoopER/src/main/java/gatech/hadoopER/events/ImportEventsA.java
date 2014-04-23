package gatech.hadoopER.events;

import gatech.hadoopER.importer.ImporterJson;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author eric
 */
public class ImportEventsA extends ImporterJson<SampleEventA, GlobalEvent> {

    @Override
    protected void map(SampleEventA from, GlobalEvent to) {
        to.title = from.name;
        to.datetime = from.date + from.time;
        to.location = from.location;
        to.other = from.description;
    }

    @Override
    protected Class<SampleEventA> getFromClass() {
        return SampleEventA.class;
    }

    @Override
    public Path getInputPath() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}
