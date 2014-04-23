/*
 * CS 4365 Project
 */
package gatech.hadoopER.products;

import gatech.hadoopER.Runner;
import gatech.hadoopER.builder.Builder;
import gatech.hadoopER.combiner.Combiner;
import gatech.hadoopER.importer.Importer;
import gatech.hadoopER.products.GlobalProduct.GPArrayWritable;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author eric
 */
public class ProductRunner extends Runner<GlobalProduct, GPArrayWritable> {

    public static void main(String[] args) throws Exception {
        start(new ProductRunner(), args);
    }

    @Override
    public Class<GlobalProduct> getToClass() {
        return GlobalProduct.class;
    }

    @Override
    public Class<GPArrayWritable> getToArrayClass() {
        return GPArrayWritable.class;
    }

    @Override
    public List<Importer> getImporters() {
        Importer[] importers = {new ImportAmazon()};
        return Arrays.asList(importers);
    }

    @Override
    public Builder<GlobalProduct> getBuilder() {
        return new ProductBuilder();
    }

    @Override
    public Combiner<GlobalProduct> getCombiner() {
        return new ProductCombiner();
    }

    @Override
    public Path getHome() {
        return new Path("/user/epeyton.site/products/");
    }

}
