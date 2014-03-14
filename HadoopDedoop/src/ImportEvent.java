/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

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
