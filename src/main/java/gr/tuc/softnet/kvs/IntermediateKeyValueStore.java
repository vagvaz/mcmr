package gr.tuc.softnet.kvs;

import java.util.Iterator;

/**
 * Created by ap0n on 4/4/2016.
 */
public interface IntermediateKeyValueStore<K, V> extends KeyValueStore<K, Iterator<V>> {

  void append(K key, V value);
}