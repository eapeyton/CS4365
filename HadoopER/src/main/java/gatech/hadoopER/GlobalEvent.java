package gatech.hadoopER;

import gatech.hadoopER.importer.To;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 *
 * @author eric
 */
public class GlobalEvent extends To {

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 89 * hash + Objects.hashCode(this.title);
        hash = 89 * hash + Objects.hashCode(this.datetime);
        hash = 89 * hash + Objects.hashCode(this.location);
        hash = 89 * hash + Objects.hashCode(this.city);
        hash = 89 * hash + Objects.hashCode(this.other);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final GlobalEvent other = (GlobalEvent) obj;
        return true;
    }

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
        return new HashSet<>(Arrays.asList(title.split(" ")));
    }
}
