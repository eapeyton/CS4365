package gatech.hadoopER.global;

import gatech.hadoopER.io.SelfSerializingWritable;
import java.util.Set;
import java.util.UUID;

/**
 * To is the base class for the global schema of an entity type.
 * @author eric
 */
public abstract class To extends SelfSerializingWritable {

    public UUID uuid;

    public abstract Set<String> getBlockingKeys();

}
