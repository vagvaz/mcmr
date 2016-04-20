package gr.tuc.softnet.kvs;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Created by ap0n on 20/4/2016.
 */
public class MapDBMultiKVSValuesIterator<K extends WritableComparable, V extends Writable>
    implements Iterator<V> {

  private Iterator<Map.Entry<KeyWrapper<K>, V>> dataIterator;
  private int valuesCount;

  public MapDBMultiKVSValuesIterator(
      Iterator<Map.Entry<KeyWrapper<K>, V>> dataIterator, int valuesCount) {
    this.dataIterator = dataIterator;
    this.valuesCount = valuesCount;
  }

  @Override
  public boolean hasNext() {
    return valuesCount >= 0;  // valuesCount starts from 0
  }

  @Override
  public V next() {
    if (valuesCount < 0) {
      throw new NoSuchElementException();
    }
    --valuesCount;
    assert (dataIterator.hasNext());
    return dataIterator.next().getValue();
  }
}
