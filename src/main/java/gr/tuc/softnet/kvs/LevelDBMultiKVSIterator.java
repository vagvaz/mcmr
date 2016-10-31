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
import org.iq80.leveldb.ReadOptions;

import java.io.IOException;
import java.math.BigInteger;
import java.util.*;

/**
 * Created by ap0n on 7/4/2016.
 */
public class LevelDBMultiKVSIterator<K extends WritableComparable, V extends Writable>
  implements Iterable<Map.Entry<K, Iterator<V>>>, Iterator<Map.Entry<K, Iterator<V>>> {

  private DB keysDB;
  private DB dataDB;
  private DBIterator keysIterator;
  private DBIterator dataIterator;
  private Class<K> keyClass;
  private Class<V> valueClass;
  private Map.Entry<byte[], byte[]> currentEntry;
  private Configuration hadoopConfiguration;
  private ReadOptions readOptions;

  public LevelDBMultiKVSIterator(DB keysDB, DB dataDB, Class<K> keyClass, Class<V> valueClass) {
    this.keysDB = keysDB;
    this.dataDB = dataDB;
    this.keyClass = keyClass;
    this.valueClass = valueClass;
    keysIterator = keysDB.iterator();
    keysIterator.seekToFirst();
    readOptions = new ReadOptions();
    readOptions.fillCache(false);
    //    dataIterator = dataDB.iterator(readOptions);
    dataIterator = dataDB.iterator();
    dataIterator.seekToFirst();
    currentEntry = null;
    hadoopConfiguration = new Configuration();
  }

  @Override public Iterator<Map.Entry<K, Iterator<V>>> iterator() {

//    System.out.println("Keys:");
//    while (keysIterator.hasNext()) {
//      Map.Entry<byte[], byte[]> keysEntry = keysIterator.next();
//      K key = ReflectionUtils.newInstance(keyClass, hadoopConfiguration);
//      ByteArrayDataInput input = ByteStreams.newDataInput(keysEntry.getKey());
//      try {
//        key.readFields(input);
//      } catch (IOException e) {
//        e.printStackTrace();
//      }
//      System.out.println("key = " + key.toString());
//      System.out.println("count = " + Integer.parseInt(new String(keysEntry.getValue())));
//    }
//
////    System.out.println("Values: ");
////    while (dataIterator.hasNext()) {
////      Map.Entry<byte[], byte[]> dataEntry = dataIterator.next();
////
////      byte[] wrapperBytes = dataEntry.getKey();
////      KeyWrapper<K> wrapper = new KeyWrapper<>(keyClass);
////      ByteArrayDataInput wrapperInput = ByteStreams.newDataInput(wrapperBytes);
////      try {
////        wrapper.readFields(wrapperInput);
////      } catch (IOException e) {
////        e.printStackTrace();
////      }
////
////      V value = ReflectionUtils.newInstance(valueClass, hadoopConfiguration);
////      ByteArrayDataInput input = ByteStreams.newDataInput(dataEntry.getValue());
////      try {
////        value.readFields(input);
////      } catch (IOException e) {
////        e.printStackTrace();
////      }
////      System.out.println("value = " + value.toString());
////    }
    keysIterator.seekToFirst();
    dataIterator.seekToFirst();
    return this;
  }

  @Override public boolean hasNext() {
    return keysIterator.hasNext();
  }

  //  @Override
  //  public Map.Entry<K, Iterator<V>> next() {
  //    if (!dataIterator.hasNext()) {
  //      try {
  //        dataIterator.close();
  //      } catch (IOException e) {
  //        e.printStackTrace();
  //      }
  //      throw new NoSuchElementException("LeveldMulti KVS ");
  //    }
  //
  //    if (currentEntry == null) {
  //      currentEntry = dataIterator.next();
  //    }
  //
  //    Set<V> values = null;
  //    K currentKey = null;
  //    try {
  //      currentKey = getFromBytes(keysIterator.next().getKey(), keyClass);
  //
  //      values = new HashSet<>();
  //
  //      while (currentKey == getFromBytes(currentEntry.getKey(), keyClass)) {
  //        values.add(getFromBytes(currentEntry.getValue(), valueClass));
  //        if (!dataIterator.hasNext()) {
  //          break;
  //        }
  //        currentEntry = dataIterator.next();
  //      }
  //    } catch (IOException e) {
  //      e.printStackTrace();
  //    }
  //
  //    if (values == null) {
  //      return null;
  //    }
  //
  //    return new AbstractMap.SimpleEntry<>(currentKey, values.iterator());
  //  }

  @Override public Map.Entry<K, Iterator<V>> next() {
    if (!keysIterator.hasNext()) {
      try {
        keysIterator.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
      throw new NoSuchElementException("LeveldMulti KVS ");
    }

    currentEntry = keysIterator.next();
    if (currentEntry == null) {
      currentEntry = dataIterator.next();
    }


    K currentKey = null;
    Iterator<V> iterator = null;
    try {
      currentKey = getFromBytes(currentEntry.getKey(), keyClass);
      Integer integer = Integer.parseInt(new String(currentEntry.getValue()));
      iterator =
        new LeveldBMultiKVSValuesIterator<K, V>(dataIterator, currentKey, integer, keyClass,
          valueClass);


    } catch (IOException e) {
      e.printStackTrace();
    }

    return new AbstractMap.SimpleEntry<>(currentKey, iterator);
  }

  private <T extends Writable> T getFromBytes(byte[] bytes, Class<T> tClass) throws IOException {
    ByteArrayDataInput input = ByteStreams.newDataInput(bytes);
    T result = ReflectionUtils.newInstance(tClass, hadoopConfiguration);
    result.readFields(input);
    return result;
  }
}
