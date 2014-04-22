package gatech.hadoopER.importer;

import gatech.hadoopER.io.SelfSerializingWritable;
import java.util.Set;
import java.util.UUID;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author eric
 */
public abstract class To extends SelfSerializingWritable {
    public UUID uuid;
    private boolean isGrouped;

    public boolean isGrouped() {
        return isGrouped;
    }

    public void setGrouped(boolean isGrouped) {
        this.isGrouped = isGrouped;
    }
    
    public abstract Set<String> getBlockingKeys();
    
    
}
