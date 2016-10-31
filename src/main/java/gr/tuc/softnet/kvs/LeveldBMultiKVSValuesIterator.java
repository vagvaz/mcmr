package gr.tuc.softnet.kvs;

import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteStreams;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.ReflectionUtils;
import org.iq80.leveldb.DBIterator;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Created by vagvaz on 08/10/16.
 */
public class LeveldBMultiKVSValuesIterator<K extends WritableComparable, V extends Writable> implements Iterator<V> {
  private DBIterator dataIterator;
  private Integer valuesCount;
  private K key;
  private Configuration hadoopConfiguration;
  private Class<K> keyClass;
  private Class<V> valueClass;
  public LeveldBMultiKVSValuesIterator(DBIterator dataIterator, K currentKey, Integer count, Class<K> keyClass, Class<V> valueClass) {
    this.dataIterator = dataIterator;
    this.key = currentKey;
    this.valuesCount = count;
    this.keyClass = keyClass;
    this.valueClass = valueClass;
    System.out.println("kk: " + key + " vv: " + valuesCount);
  }

  @Override public boolean hasNext() {
    return valuesCount >= 0;
  }

  @Override public V next() {
    if(valuesCount < 0){
      throw new NoSuchElementException("Leveldb values iterator");
    }
    --valuesCount;
    assert(dataIterator.hasNext());
    Map.Entry<byte[],byte[]> entry = dataIterator.next();
    try {
      KeyWrapper<K> wrapper  = new KeyWrapper(keyClass);
      ByteArrayDataInput input = ByteStreams.newDataInput(entry.getKey());
      wrapper.readFields(input);
      if(!wrapper.getKey().equals(key)){
        System.err.println("FSSTA " + wrapper.getKey().toString() + " " + key.toString() + " " + wrapper.getCounter());
        System.exit(0);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    V value =  null;
    try {
      value = getFromBytes(entry.getValue(),valueClass);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return value;
  }

  private <T extends Writable> T getFromBytes(byte[] bytes, Class<T> tClass) throws IOException {
    ByteArrayDataInput input = ByteStreams.newDataInput(bytes);
    T result = ReflectionUtils.newInstance(tClass, hadoopConfiguration);
    result.readFields(input);
    return result;
  }
}
