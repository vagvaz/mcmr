package gr.tuc.softnet.kvs;

import org.apache.commons.collections.map.HashedMap;
import rx.Observable;
import rx.Subscriber;

import java.util.Iterator;
import java.util.Map;

/**
 * Created by vagvaz on 03/04/16.
 */
public class TestKVS<K,V> implements KeyValueStore<K,V> {
    Map<K,V> map;
    public TestKVS(){
        super();
        map = new HashedMap();
    }
    @Override
    public void flush() {

    }

    @Override
    public void put(K key, V value) {
        map.put(key,value);
    }

    @Override
    public V get(K key) {
       return map.get(key);
    }

    @Override
    public int size() {
       return  map.size();
    }

    @Override
    public Iterable<Map.Entry<K, Integer>> getKeysIterator() {
        return null;
    }

    @Override
    public Iterator<V> getKeyIterator(K key, Integer counter) {
        return null;
    }

    @Override
    public Iterable<Map.Entry<K, V>> iterator() {
      return  map.entrySet();
    }

    @Override
    public boolean contains(K key) {
        return map.containsKey(key);
    }

    @Override
    public void close() {

    }

    @Override
    public String getName() {
        return "testInput";
    }

    @Override
    public void call(Subscriber<? super Map.Entry<K, V>> subscriber) {
        Observable.from(this.iterator()).subscribe(subscriber);
    }
}
