/*
 * CS 4365 Project
 */
package gatech.hadoopER.products;

import gatech.hadoopER.StringSet;
import gatech.hadoopER.importer.To;
import gatech.hadoopER.util.ERUtil;
import java.util.Objects;
import java.util.Set;
import org.apache.hadoop.io.ArrayWritable;

/**
 *
 * @author eric
 */
public class GlobalProduct extends To {

    StringSet id = new StringSet();
    StringSet name = new StringSet();
    StringSet description = new StringSet();
    StringSet manufacturer = new StringSet();
    double price;

    @Override
    public Set<String> getBlockingKeys() {
        return ERUtil.splitToWords(name);
    }

    public static class GPArrayWritable extends ArrayWritable {

        public GPArrayWritable() {
            super(GlobalProduct.class);
        }
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 41 * hash + Objects.hashCode(this.id);
        hash = 41 * hash + Objects.hashCode(this.name);
        hash = 41 * hash + Objects.hashCode(this.description);
        hash = 41 * hash + Objects.hashCode(this.manufacturer);
        hash = 41 * hash + (int) (Double.doubleToLongBits(this.price) ^ (Double.doubleToLongBits(this.price) >>> 32));
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
        final GlobalProduct other = (GlobalProduct) obj;
        if (!Objects.equals(this.id, other.id)) {
            return false;
        }
        if (!Objects.equals(this.name, other.name)) {
            return false;
        }
        if (!Objects.equals(this.description, other.description)) {
            return false;
        }
        if (!Objects.equals(this.manufacturer, other.manufacturer)) {
            return false;
        }
        if (Double.doubleToLongBits(this.price) != Double.doubleToLongBits(other.price)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "GlobalProduct{" + "id=" + id + ", name=" + name + ", description=" + description + ", manufacturer=" + manufacturer + ", price=" + price + '}';
    }

}
