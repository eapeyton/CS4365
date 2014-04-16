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
        from.address = "address";
        from.date = "date";
        from.description = "description";
        from.location = "location";
        from.name = "name";
        from.time = "time";
        for (Entry<String,String> entry: value.entrySet()) {
            Logger.getLogger("Mapper").info("Key:" + entry.getKey().toString() + " - Value:" + entry.getValue());
        }
    }
    
}
