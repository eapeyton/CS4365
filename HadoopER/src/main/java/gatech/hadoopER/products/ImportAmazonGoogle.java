/*
 * CS 4365 Project
 */
package gatech.hadoopER.products;

import gatech.hadoopER.importer.From;
import gatech.hadoopER.importer.ImporterCSV;
import gatech.hadoopER.products.ImportAmazonGoogle.AmazonGoogleProduct;
import java.util.List;
import org.apache.hadoop.fs.Path;

/**
 * Example importer of both Google and Amazon datasets (both have same schema).
 * @author eric
 */
public class ImportAmazonGoogle extends ImporterCSV<AmazonGoogleProduct, GlobalProduct> {

    @Override
    protected void csvToFrom(List<String> cols, AmazonGoogleProduct from) {
        from.id = cols.get(0);
        from.title = cols.get(1).replaceAll("\"", "");
        from.description = cols.get(2).replaceAll("\"", "");
        from.manufacturer = cols.get(3).replaceAll("\"", "");
        from.price = Double.parseDouble(cols.get(4));
    }

    @Override
    protected Class<AmazonGoogleProduct> getFromClass() {
        return AmazonGoogleProduct.class;
    }

    @Override
    protected void map(AmazonGoogleProduct from, GlobalProduct to) {
        to.id.add(from.id);
        to.name.add(from.title);
        to.description.add(from.description);
        to.manufacturer.add(from.manufacturer);
        to.price = from.price;
    }

    @Override
    public Path getInputPath() {
        return new Path("/user/epeyton.site/products/input-med/20products.csv");
    }

    public static class AmazonGoogleProduct extends From {

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
