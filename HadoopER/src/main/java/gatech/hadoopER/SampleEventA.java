package gatech.hadoopER;

/**
 *
 * @author eric
 */
public class SampleEventA extends From {

    @Override
    public String toString() {
        return "SampleEventA{" + "name=" + name + ", date=" + date + ", time=" + time + ", location=" + location + ", address=" + address + ", description=" + description + '}';
    }
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
