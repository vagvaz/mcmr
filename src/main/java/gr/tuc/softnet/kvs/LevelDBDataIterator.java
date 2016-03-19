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

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Created by vagvaz on 8/17/15.
 */
public class LevelDBDataIterator<K extends WritableComparable,V extends Writable> implements Iterator<V> {
  DB data;
  K key;
  V defaultInstance;
  KeyWrapper<K> currentKey;
  Integer total;
  int currentCounter;
  DBIterator iterator;
  ReadOptions readOptions;
  //    private BasicBSONDecoder decoder = new BasicBSONDecoder();
  Class<K> keyClass;
  Class<V> valueClass;



  public LevelDBDataIterator(DB dataDB, K key, Integer counter, Class<V> valueClass) {
    this.data = dataDB;
    this.key = key;
    this.keyClass = (Class<K>) key.getClass();
    this.valueClass = valueClass;
    this.defaultInstance = ReflectionUtils.newInstance(valueClass,new Configuration());
    this.currentKey = new KeyWrapper<K>(keyClass);
    this.total = counter;
    readOptions = new ReadOptions();
  }

  public synchronized boolean hasNext() {
    if (currentCounter <= total) {
      return true;
    }
    //        try {
    //            iterator.close();
    //        } catch (IOException e) {
    //            e.printStackTrace();
    //        }
    return false;
  }

  public synchronized V next() {
    if (currentCounter <= total) {
      Map.Entry<byte[], byte[]> entry = iterator.next();

      if (!validateKey(entry.getKey())) {
        System.err.println("SERIOUS ERRPR key " + key + " but entry " + entry.getKey());
      }

//      Tuple result = new Tuple(decoder.readObject(entry.getValue()));
      ByteArrayDataInput input = ByteStreams.newDataInput(entry.getValue());
      try {
        defaultInstance.readFields(input);
      } catch (IOException e) {
        e.printStackTrace();
      }
      currentCounter++;
      return defaultInstance;
    }
    throw new NoSuchElementException("Leveldb Iterator no more values");
  }

  private boolean validateKey(byte[] key) {
    ByteArrayDataInput  input = ByteStreams.newDataInput(key);
    try {
      currentKey.readFields(input);
    } catch (IOException e) {
      e.printStackTrace();
    }
    if (currentKey.getKey().compareTo(this.key) == 0) {
      return true;
    }
    return false;
  }

  private String getKey(byte[] key) {
    String result = new String(key);
    return result.substring(0,result.lastIndexOf("{}"));
  }

  @Override public void remove() {

  }

  public void initialize(K key, int tot) {
    this.key = key;
    this.total = tot;
    this.currentCounter = 0;
    //        if(iterator!=null)
    //        reportState(key,tot);
    if (iterator == null) {
      iterator = data.iterator(readOptions.fillCache(false));
      iterator.seekToFirst();
      //            reportState(key,tot);
      Map.Entry<byte[], byte[]> entry = iterator.peekNext();
      if (!validateKey(entry.getKey())) {
        System.out.println("Unsuccessful for key " + this.key + " was " + new String(entry.getKey()));
        String searchKey = key + "{}";
        iterator.seek(searchKey.getBytes());
      }
      return;
    }
    Map.Entry<byte[], byte[]> entry = iterator.peekNext();
    if (!validateKey(entry.getKey())) {
      System.out.println("Unsuccessful for key " + this.key + " was " + new String(entry.getKey()));
      String searchKey = key + "{}";
      iterator.seek(searchKey.getBytes());
    }


  }

  private void reportState(String key, int tot) {
    System.err.println("key=" + key + " totalnum= " + tot);
    System.err.println("it has next " + iterator.hasNext());
    if (iterator.hasNext()) {
      System.err.println("Next key is =" + new String(iterator.peekNext().getKey()));
    }
  }

  public synchronized void close() {
    try {
      iterator.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
