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

    void put(K key, V value) throws Exception;
    V get(K key);
    int size();
    Iterable<Map.Entry<K, Integer>> getKeysIterator();  // Don't implement
    Iterator<V> getKeyIterator(K key, Integer counter);  // Don't implement
    Iterable<Map.Entry<K, V>> iterator();  // Implement for map & level db (see LevelDBIndex.call)
                                           // and for memory (for pipeline) keep subscribers and
                                           // call subscriber.onNext every X puts
    boolean contains(K key);
    void close();
    String getName();
}

/*
* Implement 6 classes implementing this interface for
* - mapdb
* - level db
* - nothing
* (key, value) & (key, list-of-values) for each ^
* for the list-of-values extend this interface with a new one.
* */