package gatech.hadoopdedoopmaven;

import org.apache.hadoop.io.MapWritable;

/**
 *
 * @author eric
 */
public class ImportEvent extends ImporterJson<EventsOne,GlobalEvent> {

    @Override
    protected void map(EventsOne from, GlobalEvent to) {
        to.title = from.name;
        to.datetime = from.date + from.time;
        to.location = from.location;
        to.other = from.description;
    }

    @Override
    protected Class<EventsOne> getFrom() {
        return EventsOne.class;
    }

    @Override
    protected Class<GlobalEvent> getTo() {
        return GlobalEvent.class;
    }

    @Override
    protected void mapToFrom(MapWritable value, EventsOne from) {
        
    }
    
}
