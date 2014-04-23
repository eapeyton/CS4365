/*
 * CS 4365 Project
 */
package gatech.hadoopER.products;

import gatech.hadoopER.builder.Builder;
import gatech.hadoopER.util.ERUtil;

/**
 *
 * @author eric
 */
public class ProductBuilder extends Builder<GlobalProduct> {

    @Override
    protected boolean areMatching(GlobalProduct a, GlobalProduct b) {
        if(ERUtil.computeJaccardOfWords(a.name, b.name) > .5 && ERUtil.getPercentDifference(a.price, b.price) < .2) {
            return true;
        }
        return false;
    }

}
