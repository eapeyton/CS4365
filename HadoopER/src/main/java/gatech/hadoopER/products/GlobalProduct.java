/*
 * CS 4365 Project
 */

package gatech.hadoopER.products;

import gatech.hadoopER.importer.To;
import java.util.Set;

/**
 *
 * @author eric
 */
public class GlobalProduct extends To {
    
    String id;
    String name;
    String description;
    String manufacturer;
    double price;

    @Override
    public Set<String> getBlockingKeys() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
}
