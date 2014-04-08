package gatech.hadoopdedoopmaven;

/**
 *
 * @author eric
 */
public class EventsOne extends From {
    String name;
    String date;
    String time;
    String location;
    String address;
    String description;

    @Override
    protected String getKey() {
        return name + date + time;
    }
}
