package gatech.hadoopdedoopmaven;

import java.util.Map;
import java.util.Map.Entry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 *
 * @author eric
 */
public class ImportEvent extends ImporterJson<EventsOne,GlobalEvent> {
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int res = ToolRunner.run(conf, new ImportEvent(), args);
    }

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
    protected void mapToFrom(Map<String,String> value, EventsOne from) {
        from.name = value.get("Name");
        from.date = value.get("Date");
        from.time = value.get("Time");
        from.location = value.get("Location");
        from.address = value.get("Address");
        from.description = value.get("Description");
    }
    
}
