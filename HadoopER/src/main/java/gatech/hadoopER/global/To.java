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

    /**
     * Return the set of strings to utilize during blocking.
     * @return the blocking keys
     */
    public abstract Set<String> getBlockingKeys();

}
