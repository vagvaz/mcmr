package gr.tuc.softnet.kvs;

import java.util.Iterator;
import java.util.Map;

/**
 * Created by vagvaz on 25/02/16.
 */
public class PipelinedKeyValueStore<K, V> implements KeyValueStore<K, V>  {
    public PipelinedKeyValueStore(String defaultBaseDir, String name) {
    }

    @Override
    public void flush() {

    }

    public void put(K key, V value) {

    }

    public V get(K key) {
        return null;
    }

    public int size() {
        return 0;
    }

    @Override
    public Iterable<Map.Entry<K, Integer>> getKeysIterator() {
        return null;
    }

    @Override
    public Iterator<V> getKeyIterator(K key, Integer counter) {
        return null;
    }

    public Iterator<K> keysIterator() {
        return null;
    }

    public Iterator<V> valuesIterator() {
        return null;
    }

    public Iterator<Map.Entry<K, V>> iterator() {
        return null;
    }

    public boolean contains(K key) {
        return false;
    }

    public void close() {

    }

    @Override
    public String getName() {
        return null;
    }
}
