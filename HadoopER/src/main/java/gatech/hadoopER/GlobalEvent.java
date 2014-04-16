package gatech.hadoopER;

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
