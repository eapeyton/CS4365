/*
 * CS 4365 Project
 */
package gatech.hadoopER.products;

import gatech.hadoopER.importer.To;
import gatech.hadoopER.util.ERUtil;
import java.util.Set;
import org.apache.hadoop.io.ArrayWritable;

/**
 *
 * @author eric
 */
public class GlobalProduct extends To {

    ;

    @Override
    public String toString() {
        return "GlobalProduct{" + "id=" + id + ", name=" + name + ", description=" + description + ", manufacturer=" + manufacturer + ", price=" + price + '}';
    }

    String id;
    String name;
    String description;
    String manufacturer;
    double price;

    @Override
    public Set<String> getBlockingKeys() {
        return ERUtil.splitWords(name);
    }

    public static class GPArrayWritable extends ArrayWritable {

        public GPArrayWritable() {
            super(GlobalProduct.class);
        }
    }

}
