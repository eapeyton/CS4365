package gatech.hadoopER.importer;

import gatech.hadoopER.io.SelfSerializingWritable;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author eric
 */
public abstract class From extends SelfSerializingWritable {

    public abstract String getKey();

    public void readMap(Map<String, String> map) {
        Field[] fields = this.getClass().getDeclaredFields();
        for (Field field : fields) {
            if (map.containsKey(field.getName())) {
                try {
                    field.setAccessible(true);
                    field.set(this, map.get(field.getName()));
                } catch (IllegalArgumentException | IllegalAccessException ex) {
                    Logger.getLogger(ImporterJson.class.getName()).log(Level.SEVERE, null, ex);
                }
            }

        }
    }
}
