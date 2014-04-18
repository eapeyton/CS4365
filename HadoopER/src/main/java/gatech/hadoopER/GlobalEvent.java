package gatech.hadoopER;

import gatech.hadoopER.importer.To;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 *
 * @author eric
 */
public class GlobalEvent extends To {

    @Override
    public String toString() {
        return "GlobalEvent{" + "title=" + title + ", datetime=" + datetime + ", location=" + location + ", other=" + other + '}';
    }
    String title;
    String datetime;
    String location;
    String city;
    String other;

    @Override
    public Set<String> getBlockingKeys() {
        return new HashSet<>(Arrays.asList(city.split(" ")));
    }
}
