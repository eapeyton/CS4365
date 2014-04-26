/*
 * CS 4365 Project
 */
package gatech.hadoopER.products;

import gatech.hadoopER.combiner.Combiner;
import java.util.List;

/**
 * Example product combiner.
 * @author eric
 */
public class ProductCombiner extends Combiner<GlobalProduct> {

    @Override
    public GlobalProduct combine(List<GlobalProduct> entities) {
        GlobalProduct product = new GlobalProduct();
        double avgPrice = 0;
        for(GlobalProduct entity: entities) {
            product.name.addAll(entity.name);
            product.description.addAll(entity.description);
            product.manufacturer.addAll(entity.manufacturer);
            product.id.addAll(entity.id);
            avgPrice += entity.price;
        }
        product.price = avgPrice / entities.size();
        return product;
    }

}
