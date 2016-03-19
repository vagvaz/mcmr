package gr.tuc.softnet.kvs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

/**
 * Created by vagvaz on 27/02/16.
 */
public class KeyWrapper<K extends WritableComparable> implements WritableComparable, Serializable {
    private K key;
    private int counter;
    private Class<K> keyClass;

    public Class<K> getKeyClass() {
        return keyClass;
    }

    public void setKeyClass(Class<K> keyClass) {
        this.keyClass = keyClass;
    }

    public int getCounter() {
        return counter;
    }

    public void setCounter(int counter) {
        this.counter = counter;
    }


    public KeyWrapper(Class<K> keyClass){
        this.keyClass = keyClass;
        key = ReflectionUtils.newInstance(keyClass, new Configuration());
        counter = -1;
    }
    public KeyWrapper(K key, int counter){
        this.keyClass = (Class<K>) key.getClass();
        this.key = key;
        this.counter = counter;
    }

    public int compareTo(KeyWrapper<K> k){
        if(k == null){
            return -1;
        }
        if(this.key == null){
            return 1;
        }
        int result = key.compareTo(k);
        if(result == 0){
            return Integer.compare(counter,k.counter);
        }
        return 0;
    }

    public void write(DataOutput dataOutput) throws IOException {
        key.write(dataOutput);
        dataOutput.writeInt(counter);
    }

    public void readFields(DataInput dataInput) throws IOException {
        key.readFields(dataInput);
        counter = dataInput.readInt();
    }

    public K getKey() {
        return key;
    }

    private void writeObject(java.io.ObjectOutputStream out)
            throws IOException {
        out.writeObject(keyClass);
        key.write(out);
    }

    private void readObject(java.io.ObjectInputStream in) {
        try {
            keyClass = (Class<K>) in.readObject();
            key.readFields(in);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }



    }

    private void readObjectNoData() {
        keyClass = null;
        key = null;
        counter = -1;
    }

    @Override
    public int compareTo(Object o) {
        KeyWrapper<K> k = (KeyWrapper<K>) o;
        if(k == null){
            return -1;
        }
        if(this.key == null){
            return 1;
        }
        int result = key.compareTo(k.key);
        if(result == 0){
            return Integer.compare(counter,k.counter);
        }
        return result;
    }
    @Override
    public String toString(){
        return key.toString()+counter;
    }
}
