package gr.tuc.softnet.kvs;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.mapdb.BTreeMap;

import java.util.AbstractMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Created by ap0n on 7/4/2016.
 */
public class MapDBMultiKVSIterator<K extends WritableComparable, V extends Writable>
    implements Iterable<Map.Entry<K, Iterator<V>>>,
               Iterator<Map.Entry<K, Iterator<V>>> {

  private BTreeMap<KeyWrapper<K>, V> dataDB;
  private BTreeMap<K, Integer> keysDB;
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

  @Override
  public Iterator<Map.Entry<K, Iterator<V>>> iterator() {
    return this;
  }

  @Override
  public boolean hasNext() {
    return keysIterator.hasNext();
  }

  @Override
  public Map.Entry<K, Iterator<V>> next() {

    if (!dataIterator.hasNext()) {
      throw new NoSuchElementException();
    }

    Map.Entry<K, Integer> currentKeyWrapper = keysIterator.next();

    MapDBMultiKVSValuesIterator<K, V> valuesIterator = new MapDBMultiKVSValuesIterator<>(
        dataIterator, currentKeyWrapper.getValue());

    return new AbstractMap.SimpleEntry<>(currentKeyWrapper.getKey(), valuesIterator);
  }
}
