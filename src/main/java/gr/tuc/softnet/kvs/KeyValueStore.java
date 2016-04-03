package gr.tuc.softnet.kvs;

import rx.Observable;

import java.util.Iterator;
import java.util.Map;

/**
 * Created by vagvaz on 02/02/16.
 */
public interface KeyValueStore<K,V> extends Observable.OnSubscribe<Map.Entry<K,V>> {
    //    80.156.73.113:11222;80.156.73.116:11222;80.156.73.123:11222;80.156.73.128:11222
    //    ;
    void flush();

    void put(K key, V value);
    V get(K key);
    int size();
    Iterable<Map.Entry<K, Integer>> getKeysIterator();
    Iterator<V> getKeyIterator(K key, Integer counter);
    Iterable<Map.Entry<K, V>> iterator();
    boolean contains(K key);
    void close();
    String getName();
}
