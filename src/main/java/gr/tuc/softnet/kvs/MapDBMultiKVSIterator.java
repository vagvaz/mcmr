package gr.tuc.softnet.kvs;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.mapdb.BTreeMap;

import java.util.*;

/**
 * Created by ap0n on 7/4/2016.
 */
public class MapDBMultiKVSIterator<K extends WritableComparable, V extends Writable>
    implements Iterable<Map.Entry<K, Iterator<V>>>,
               Iterator<Map.Entry<K, Iterator<V>>> {

  private Map<KeyWrapper<K>, V> dataDB;
  private Map<K, Integer> keysDB;
  private Iterator<Map.Entry<KeyWrapper<K>, V>> dataIterator;
  private Iterator<Map.Entry<K, Integer>> keysIterator;
  private Map.Entry<KeyWrapper<K>, V> currentEntry;

  public MapDBMultiKVSIterator(BTreeMap<KeyWrapper<K>, V> dataDB, BTreeMap<K, Integer> keysDB) {
    this.dataDB = dataDB;
    this.keysDB = keysDB;
    dataIterator = dataDB.entrySet().iterator();
    keysIterator = keysDB.entrySet().iterator();
    currentEntry = null;
  }
  public MapDBMultiKVSIterator(Map<KeyWrapper<K>, V> dataDB, Map<K, Integer> keysDB) {
    this.dataDB = dataDB;
    this.keysDB = keysDB;
    dataIterator = dataDB.entrySet().iterator();
    keysIterator = keysDB.entrySet().iterator();
    currentEntry = null;
  }

  @Override
  public Iterator<Map.Entry<K, Iterator<V>>> iterator() {
    return this;
  }

  @Override
  public boolean hasNext() {
    return keysIterator.hasNext();
  }

  @Override
synchronized public Map.Entry<K, Iterator<V>> next() {

    if (!keysIterator.hasNext()) {
      throw new NoSuchElementException();
    }

    Map.Entry<K, Integer> currentKeyWrapper = keysIterator.next();

    MapDBMultiKVSValuesIterator<K, V> valuesIterator = new MapDBMultiKVSValuesIterator<>(
        dataIterator, currentKeyWrapper.getValue());

    return new AbstractMap.SimpleEntry<>(currentKeyWrapper.getKey(), valuesIterator);
  }
}
