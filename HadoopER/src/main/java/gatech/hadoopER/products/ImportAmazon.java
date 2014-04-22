/*
 * CS 4365 Project
 */

package gatech.hadoopER.products;

import gatech.hadoopER.importer.From;
import gatech.hadoopER.importer.ImporterCSV;
import gatech.hadoopER.products.ImportAmazon.AmazonProduct;
import java.util.List;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author eric
 */
public class ImportAmazon extends ImporterCSV<AmazonProduct,GlobalProduct> {

    @Override
    protected void csvToFrom(List<String> cols, AmazonProduct from) {
        from.id = cols.get(0);
        from.title = cols.get(1);
        from.description = cols.get(2);
        from.manufacturer = cols.get(3);
        from.price = Double.parseDouble(cols.get(4));
    }

    @Override
    protected Class<AmazonProduct> getFromClass() {
        return AmazonProduct.class;
    }

    @Override
    protected void map(AmazonProduct from, GlobalProduct to) {
        to.id = from.id;
        to.name = from.title;
        to.description = from.description;
        to.manufacturer = from.manufacturer;
        to.price = from.price;
    }

    @Override
    public Path getInputPath() {
        return new Path("/user/epeyton.site/products/input/");
    }
    
    public class AmazonProduct extends From {
        
        String id;
        String title;
        String description;
        String manufacturer;
        double price;

        @Override
        public String getKey() {
            return id;
        }
        
    }
}
