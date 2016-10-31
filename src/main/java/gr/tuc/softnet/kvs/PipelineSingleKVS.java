package gr.tuc.softnet.kvs;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import org.apache.commons.lang.NotImplementedException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import rx.Subscriber;

/**
 * Created by ap0n on 4/4/2016.
 */
public class PipelineSingleKVS<K extends WritableComparable, V extends Writable>
    implements KeyValueStore<K, V> {

  private Map<K, V> kvs;  // lock it for pipeline only
  private List<Subscriber<? super Map.Entry<K, V>>> subscribers;
  private KVSConfiguration configuration;

  public PipelineSingleKVS(KVSConfiguration configuration) {
    this.kvs = new HashMap<>();
    this.configuration = configuration;
    this.subscribers = new ArrayList<>();
  }

  @Override
  public void flush() {
    doFlush();
    close();
  }

  @Override
  public void put(K key, V value) {
    synchronized (kvs) {
      kvs.put(key, value);
    }
    if (kvs.size() >= configuration.getBatchSize()) {
      doFlush();
    }
  }

  @Override
  public V get(K key) {
    return kvs.get(key);
  }

  @Override
  public int size() {
    return kvs.size();
  }

  @Override
  public Iterable<Map.Entry<K, Integer>> getKeysIterator() {
    throw new NotImplementedException();
  }

  @Override
  public Iterator<V> getKeyIterator(K key, Integer counter) {
    throw new NotImplementedException();
  }

  @Override
  public Iterable<Map.Entry<K, V>> iterator() {
    return kvs.entrySet();
  }

  @Override
  public boolean contains(K key) {
    return kvs.containsKey(key);
  }

  @Override
  public void close() {
    doFlush();
    for (Subscriber<? super Map.Entry<K, V>> s : subscribers) {
      s.onCompleted();
    }
  }

  @Override
  public String getName() {
    return configuration.getName();
  }

  @Override
  public void call(Subscriber<? super Map.Entry<K, V>> subscriber) {
    subscribers.add(subscriber);
  }

  private void doFlush() {
    Map<K, V> oldKvs;
    synchronized (kvs) {
      oldKvs = kvs;
      kvs = new HashMap<>();
    }
    for (Map.Entry<K, V> e : oldKvs.entrySet()) {
      for (Subscriber<? super Map.Entry<K, V>> s : subscribers) {
        s.onNext(e);
      }
    }
  }
}
