package gr.tuc.softnet.kvs;

import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteStreams;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.ReflectionUtils;
import org.mapdb.BTreeMap;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Created by vagvaz on 10/11/15.
 */
public class MapDBDataIterator<K extends WritableComparable,V extends Writable> implements Iterator<V> {

  BTreeMap<KeyWrapper<K>,V> data;
  K key;
  KeyWrapper<K> currentKey;
  Integer total;
  int currentCounter;
  Iterator iterator;
  Class<V> valueClass;
  Class<K> keyClass;
  V defaultInstance;
  //    private BasicBSONDecoder decoder = new BasicBSONDecoder();
  public MapDBDataIterator(BTreeMap<KeyWrapper<K>, V> dataDB, K key, Integer counter, Class<V> valueClass) {
    this.data = dataDB;
    this.key = key;
    this.total = counter;
    this.valueClass = valueClass;
    defaultInstance = ReflectionUtils.newInstance(valueClass,new Configuration());
  }

   public boolean hasNext() {
    if (currentCounter <= total) {
      return true;
    }
    return false;
  }

  public V next() {
    if (currentCounter <= total) {
      Map.Entry<KeyWrapper<K>, V> entry = (Map.Entry<KeyWrapper<K>, V>) iterator.next();
      if (!validateKey(entry.getKey())) {
        System.err.println("SERIOUS ERRPR key " + key + " but entry " + entry.getKey());
      }

//      Tuple result = new Tuple(decoder.readObject(entry.getValue()));
//      ByteArrayDataInput input = ByteStreams.newDataInput(entry.getValue());
//      try {
//        defaultInstance.readFields(input);
//      } catch (IOException e) {
//        e.printStackTrace();
//      }
//      input = null;
      currentCounter++;
//      return defaultInstance;
      return entry.getValue();
    }
    throw new NoSuchElementException("MapDB Iterator no more values");
  }

  private boolean validateKey(KeyWrapper<K> key) {


    if (currentKey.getKey().compareTo(key.getKey()) == 0) {
      return true;
    }
    return false;
  }

   public void remove() {

  }

  public void initialize(K key, int tot) {
    this.key = key;
    keyClass = (Class<K>) key.getClass();
    currentKey = new KeyWrapper<K>(key,0);
    this.total = tot;
    this.currentCounter = 0;
    //        if(iterator!=null)
    //        reportState(key,tot);
    if (iterator == null) {
      iterator = data.descendingMap().entrySet().iterator();

      //      Map.Entry<String,byte[]> entry = (Map.Entry<String, byte[]>) iterator.next();
      //      if(!validateKey(entry.getKey())){
      //        System.out.println("Unsuccessful for key " + this.key + " was " + new String(entry.getKey()));
      //        String searchKey = key + "{}";
      //data.c(searchKey.getBytes());
    }
    return;
    //    }
    //    Map.Entry<String, byte[]> entry = (Map.Entry<String, byte[]>) iterator.next();
    //    if(!validateKey(entry.getKey())){
    //      System.out.println("Unsuccessful for key " + this.key + " was " + new String(entry.getKey()));
    //      String searchKey = key + "{}";
    //    }


  }

  private void reportState(String key, int tot) {
    //    iterator
  }

  public void close() {
    //    try {
    //      iterator.close();
    //    } catch (IOException e) {
    //      e.printStackTrace();
    //    }
  }
}
