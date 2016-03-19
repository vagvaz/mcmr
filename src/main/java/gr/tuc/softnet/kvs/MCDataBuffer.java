package gr.tuc.softnet.kvs;

/**
 * Created by vagvaz on 16/02/16.
 */
public interface MCDataBuffer<K,V> {
    boolean append(K key, V value);
    void flush();
    void clear();
    byte[] toBytes();
    byte[] getBytesAndClear();
}
