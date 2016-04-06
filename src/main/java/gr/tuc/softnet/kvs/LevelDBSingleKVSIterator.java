package gr.tuc.softnet.kvs;

import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteStreams;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.ReflectionUtils;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.ReadOptions;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Spliterator;
import java.util.function.Consumer;

/**
 * Created by ap0n on 6/4/2016.
 */
public class LevelDBSingleKVSIterator<K extends WritableComparable, V extends Writable>
    implements Iterable<Map.Entry<K, V>>,
               Iterator<Map.Entry<K, V>> {
  DB db;
  DBIterator iterator;
  ReadOptions readOptions;
  Class<K> keyClass;
  Class<V> valueClass;
  K currentKey;
  V currentValue;

  public LevelDBSingleKVSIterator(DB db, Class<K> keyClass, Class<V> valueClass) {
    this.db = db;
    this.keyClass = keyClass;
    this.valueClass = valueClass;
    currentKey = ReflectionUtils.newInstance(keyClass, new Configuration());
    currentValue = ReflectionUtils.newInstance(valueClass, new Configuration());
    readOptions = new ReadOptions();
    readOptions.fillCache(false);
    //        readOptions.verifyChecksums(true);
    this.iterator = this.db.iterator(readOptions);
    this.iterator.seekToFirst();
  }

  @Override
  public Iterator<Map.Entry<K, V>> iterator() {
    return this;
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public Map.Entry<K, V> next() {
    if (!iterator.hasNext()) {
      return null;
    }

    Map.Entry<byte[], byte[]> entry = iterator.next();
    ByteArrayDataInput input = ByteStreams.newDataInput(entry.getKey());
    try {
      currentKey.readFields(input);
    } catch (IOException e) {
      e.printStackTrace();
    }
    input = ByteStreams.newDataInput(entry.getValue());
    try {
      currentValue.readFields(input);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return new AbstractMap.SimpleEntry<>(currentKey, currentValue);
  }

  @Override
  public void remove() {
    throw new NotImplementedException();
  }

  public void close() {
    try {
      iterator.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

}
