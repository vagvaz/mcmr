package gr.tuc.softnet.core;

import gr.tuc.softnet.kvs.KeyWrapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.ReflectionUtils;
import org.mapdb.BTreeKeySerializer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Comparator;

/**
 * Created by vagvaz on 25/02/16.
 */
public class WritableComparableSerializer<K extends WritableComparable> extends BTreeKeySerializer<K> implements Comparator<K>, Serializable {
    protected Class kClass;
    private Configuration conf = new Configuration();
    boolean isWrapper;
    public WritableComparableSerializer(Class<K> kClass) {
        this.kClass = kClass;
        isWrapper = false;
    }

    public WritableComparableSerializer(K tmpWrapper) {
        kClass = ((KeyWrapper)tmpWrapper).getKeyClass();
        isWrapper = true;
    }


    @Override
    public void serialize(DataOutput dataOutput, int start, int end, Object[] objects) throws IOException {
        for (int i = start; i < end; i++) {
            if(!isWrapper)
                ((WritableComparable) objects[i]).write(dataOutput);
            else{
                KeyWrapper wrapper = (KeyWrapper) objects[i];
                wrapper.getKey().write(dataOutput);
                dataOutput.writeInt(wrapper.getCounter());
            }
        }
    }

    @Override
    public Object[] deserialize(DataInput dataInput, int start, int end, int size) throws IOException {
        Object[] ret = new Object[size];
        for (int i = start; i < end; i++) {
            WritableComparable k = null;

             k = (WritableComparable) ReflectionUtils.newInstance(kClass, conf);
            k.readFields(dataInput);
             if(!isWrapper){
                 ret[i] = k;
             }else{
                 int counter = dataInput.readInt();
                 KeyWrapper wrapper = new KeyWrapper(k,counter);
                 ret[i] = wrapper;
             }


        }
        return ret;
    }

    @Override
    public Comparator<K> getComparator() {
        return this;
    }

    public int compare(K o1, K o2) {
        return o1.compareTo(o2);
    }

    private void writeObject(java.io.ObjectOutputStream out)
            throws IOException {
        out.writeObject(kClass);
        out.writeBoolean(isWrapper);
    }

    private void readObject(java.io.ObjectInputStream in) {
        try {
            kClass = (Class<K>) in.readObject();
            isWrapper = in.readBoolean();
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

