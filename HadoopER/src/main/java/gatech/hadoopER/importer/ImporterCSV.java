/*
 * CS 4365 Project
 */
package gatech.hadoopER.importer;

import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

/**
 *
 * @author eric
 */
public abstract class ImporterCSV<F extends From, T extends To> extends ImporterText<F, T> {

    protected abstract void csvToFrom(List<String> cols, F from);

    @Override
    protected void textToFrom(Text text, F from) {
        String[] cols = text.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
        try {
            csvToFrom(Arrays.asList(cols), from);
        } catch (RuntimeException ex) {
            Logger.getLogger(this.getClass()).warn("Import failed for " + text.toString(), ex);
        }
    }

}
