/*
 * CS 4365 Project
 */

package gatech.hadoopER;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InvalidClassException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Field;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author trega
 */
public abstract class SelfSerializingWritable implements Writable, Serializable {

    public byte[] getBytes() throws IOException {
        ByteArrayOutputStream memStream = new ByteArrayOutputStream();
        ObjectOutputStream serializer = null;
        byte[] myBytes;

        try {
            serializer = new ObjectOutputStream(memStream);
            serializer.writeObject(this);
            myBytes = memStream.toByteArray();
        } finally {
            if (serializer != null) {
                serializer.close();
            }

            memStream.close();
        }
        
        return myBytes;
    }

    public void write(DataOutput d) throws IOException {
        byte[] dataBytes = getBytes();
        d.writeInt(dataBytes.length);
        d.write(dataBytes);
    }

    public void readFields(DataInput di) throws IOException {
        int dataBytesSize = di.readInt();
        byte[] dataBytes = new byte[dataBytesSize];
        di.readFully(dataBytes);

        ObjectInputStream serializer = null;
        ByteArrayInputStream memStream = null;

        try {
            memStream = new ByteArrayInputStream(dataBytes);
            serializer = new ObjectInputStream(memStream);
            try {
                SelfSerializingWritable input = (SelfSerializingWritable) serializer.readObject();

                Field[] fields = this.getClass().getDeclaredFields();

                for (int i = 0; i < fields.length; ++i) {
                    Field f = fields[i];
                    f.setAccessible(true);

                    if (f.getName().equals("serialVersionUID")) {
                        long inputVersion = f.getLong(input);
                        long outputVersion = f.getLong(this);
                        if (inputVersion != outputVersion) {
                            throw new InvalidClassException("Input version UID does not match output version UID.");
                        }
                    } else {
                        f.set(this, f.get(input));
                    }

                }
            } catch (Exception ex) {
                throw new IOException(ex);
            }
        } finally {
            if (serializer != null) {
                serializer.close();
            }

            if (memStream != null) {
                memStream.close();
            }
        }
    }
}
