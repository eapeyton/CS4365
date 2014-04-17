package gatech.hadoopER;

import gatech.hadoopER.importer.To;

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
    String other;
}
