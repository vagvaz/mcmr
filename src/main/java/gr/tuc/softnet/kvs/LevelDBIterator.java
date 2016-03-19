package gr.tuc.softnet.kvs;

import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteStreams;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.ReflectionUtils;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.ReadOptions;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by vagvaz on 8/17/15.
 */
public class LevelDBIterator<K extends WritableComparable> implements Iterable<Map.Entry<K, Integer>>, Iterator<Map.Entry<K, Integer>> {
  DB db;
  DBIterator iterator;
  ReadOptions readOptions;
  Class<K> keyClass;
  K currentKey;

  public LevelDBIterator(DB keysDB, Class<K> keyClass) {
    this.db = keysDB;
    this.keyClass = keyClass;
    currentKey = ReflectionUtils.newInstance(keyClass, new Configuration());
    readOptions = new ReadOptions();
    readOptions.fillCache(false);
    //        readOptions.verifyChecksums(true);
    this.iterator = db.iterator(readOptions);
    this.iterator.seekToFirst();
    //        System.out.println( new String(this.iterator.peekPrev().getKey()));
  }

  @Override public synchronized Iterator<Map.Entry<K, Integer>> iterator() {
    return this;
  }

  @Override public synchronized boolean hasNext() {
    return iterator.hasNext();
  }

  @Override public synchronized Map.Entry<K, Integer> next() {

    Integer value;
    if (iterator.hasNext()) {
      Map.Entry<byte[], byte[]> entry = iterator.next();
      ByteArrayDataInput input = ByteStreams.newDataInput(entry.getKey());
      try {
        currentKey.readFields(input);
      } catch (IOException e) {
        e.printStackTrace();
      }
      value = Integer.parseInt(new String(entry.getValue()));

      return new AbstractMap.SimpleEntry<K, Integer>(currentKey, value);
    }
    return null;
  }

  @Override public void remove() {

  }

  public synchronized void close() {
    try {
      iterator.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
