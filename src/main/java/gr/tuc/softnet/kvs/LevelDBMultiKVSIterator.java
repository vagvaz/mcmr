package gr.tuc.softnet.kvs;

import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.ReflectionUtils;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;

import java.io.IOException;
import java.math.BigInteger;
import java.util.AbstractMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Created by ap0n on 7/4/2016.
 */
public class LevelDBMultiKVSIterator<K extends WritableComparable, V extends Writable>
    implements Iterable<Map.Entry<K, Iterator<V>>>,
               Iterator<Map.Entry<K, Iterator<V>>> {

  private DB keysDB;
  private DB dataDB;
  private DBIterator keysIterator;
  private DBIterator dataIterator;
  private Class<K> keyClass;
  private Class<V> valueClass;
  private Map.Entry<byte[], byte[]> currentEntry;

  public LevelDBMultiKVSIterator(DB keysDB, DB dataDB, Class<K> keyClass,
                                 Class<V> valueClass) {
    this.keysDB = keysDB;
    this.dataDB = dataDB;
    this.keyClass = keyClass;
    this.valueClass = valueClass;
    keysIterator = keysDB.iterator();
    keysIterator.seekToFirst();
    dataIterator = dataDB.iterator();
    dataIterator.seekToFirst();
    currentEntry = null;
  }

  @Override
  public Iterator<Map.Entry<K, Iterator<V>>> iterator() {

    System.out.println("Keys:");
    while (keysIterator.hasNext()) {
      Map.Entry<byte[], byte[]> keysEntry = keysIterator.next();
      K key = ReflectionUtils.newInstance(keyClass, new Configuration());
      ByteArrayDataInput input = ByteStreams.newDataInput(keysEntry.getKey());
      try {
        key.readFields(input);
      } catch (IOException e) {
        e.printStackTrace();
      }
      System.out.println("key = " + key.toString());
      System.out.println("count = " + new BigInteger(keysEntry.getValue()));
    }

    System.out.println("Values: ");
    while (dataIterator.hasNext()) {
      Map.Entry<byte[], byte[]> dataEntry = dataIterator.next();
      System.out.println(new BigInteger(dataEntry.getValue()));
      V value = ReflectionUtils.newInstance(valueClass, new Configuration());
      ByteArrayDataInput input = ByteStreams.newDataInput(dataEntry.getValue());
      try {
        value.readFields(input);
      } catch (IOException e) {
        e.printStackTrace();
      }
      System.out.println("value = " + value.toString());
    }

    return this;
  }

  @Override
  public boolean hasNext() {
    return keysIterator.hasNext();
  }

  @Override
  public Map.Entry<K, Iterator<V>> next() {
    if (!dataIterator.hasNext()) {
      return null;
    }

    if (currentEntry == null) {
      currentEntry = dataIterator.next();
    }

    Set<V> values = null;
    K currentKey = null;
    try {
      currentKey = getFromBytes(keysIterator.next().getKey(), keyClass);

      values = new HashSet<>();

      while (currentKey == getFromBytes(currentEntry.getKey(), keyClass)) {
        values.add(getFromBytes(currentEntry.getValue(), valueClass));
        if (!dataIterator.hasNext()) {
          break;
        }
        currentEntry = dataIterator.next();
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

    if (values == null) {
      return null;
    }

    return new AbstractMap.SimpleEntry<>(currentKey, values.iterator());
  }

  private <T extends Writable> T getFromBytes(byte[] bytes, Class<T> tClass) throws IOException {
    ByteArrayDataInput input = ByteStreams.newDataInput(bytes);
    T result = ReflectionUtils.newInstance(tClass, new Configuration());
    result.readFields(input);
    return result;
  }
}
