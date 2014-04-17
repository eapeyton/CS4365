package gatech.hadoopER;

import gatech.hadoopER.importer.ImporterJson;

/**
 *
 * @author eric
 */
public class ImportEventsA extends ImporterJson<SampleEventA,GlobalEvent> {
    
    public static void main(String[] args) throws Exception {
        new ImportEventsA().go(args);
    }

    @Override
    protected void map(SampleEventA from, GlobalEvent to) {
        to.title = from.name;
        to.datetime = from.date + from.time;
        to.location = from.location;
        to.other = from.description;
    }

    @Override
    protected Class<SampleEventA> getFrom() {
        return SampleEventA.class;
    }

    @Override
    protected Class<GlobalEvent> getTo() {
        return GlobalEvent.class;
    }
    
}
