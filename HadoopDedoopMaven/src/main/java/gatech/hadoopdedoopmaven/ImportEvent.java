package gatech.hadoopdedoopmaven;

/**
 *
 * @author eric
 */
public class ImportEvent extends Importer<EventsOne,GlobalEvent> {

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
    
}
