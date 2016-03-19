package gr.tuc.softnet.core;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.mapdb.Serializer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

/**
 * Created by vagvaz on 25/02/16.
 */
public class WritableSerializer<K extends Writable> implements Serializer<K>, Serializable {
    org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
    Class<K> kClass;
    public WritableSerializer(Class<K> kClass){
        this.kClass = kClass;
    }


    public void serialize(DataOutput out, K value) throws IOException {
        value.write(out);
    }

    public K deserialize(DataInput in, int available) throws IOException {
        K k =   (K) ReflectionUtils.newInstance(kClass,conf);
        k.readFields(in);
        return k;
    }

    public int fixedSize() {
        return -1;
    }

    private void writeObject(java.io.ObjectOutputStream out)
            throws IOException {
        out.writeObject(kClass);
    }

    private void readObject(java.io.ObjectInputStream in) {
        try {
            kClass = (Class<K>) in.readObject();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        conf = new Configuration();

    }

    private void readObjectNoData() {
        kClass = null;
        conf = new Configuration();
    }
}
