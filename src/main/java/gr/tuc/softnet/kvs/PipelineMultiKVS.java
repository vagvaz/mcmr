package gr.tuc.softnet.kvs;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import rx.Subscriber;

/**
 * Created by ap0n on 4/4/2016.
 */
public class PipelineMultiKVS<K, V> implements IntermediateKeyValueStore<K, V> {

  private Map<K, List<V>> kvs;
  private List<Subscriber<? super Map.Entry<K, Iterator<V>>>> subscribers;
  private KVSConfiguration configuration;
  private int puts;
  private AtomicInteger size;

  public PipelineMultiKVS(KVSConfiguration configuration) {
    this.configuration = configuration;
    this.kvs = new HashMap<>();
    this.puts = 0;
    this.size = new AtomicInteger(0);
    this.subscribers = new ArrayList<>();
  }

  @Override
  public void append(K key, V value) {
    List<V> list = kvs.get(key);
    if (list == null) {
      list = new LinkedList<>();
      kvs.put(key, list);
    }
    list.add(value);
    size.incrementAndGet();

    if (puts++ >= configuration.getBatchSize()) {
      flush();
    }
  }

  @Override
  public void flush() {
    Map<K, List<V>> oldKvs;
    synchronized (kvs) {
      oldKvs = kvs;
      kvs = new HashMap<>();
    }

    for (Map.Entry<K, List<V>> e : oldKvs.entrySet()) {
      size.addAndGet(-1 * e.getValue().size());  // subtract the flushed items
      for (Subscriber<? super Map.Entry<K, Iterator<V>>> s : subscribers) {
        s.onNext(new AbstractMap.SimpleEntry<>(e.getKey(), e.getValue().iterator()));
      }
    }
  }

  @Override
  public void put(K key, Iterator<V> value) {
    throw new NotImplementedException();
  }

  @Override
  public Iterator<V> get(K key) {
    throw new NotImplementedException();
  }

  @Override
  public int size() {  // TODO(ap0n): Should return the size of the map or the size of the Values?
    return size.get();
  }

  @Override
  public Iterable<Map.Entry<K, Integer>> getKeysIterator() {
    throw new NotImplementedException();
  }

  @Override
  public Iterator<Iterator<V>> getKeyIterator(K key, Integer counter) {
    throw new NotImplementedException();
  }

  @Override
  public Iterable<Map.Entry<K, Iterator<V>>> iterator() {
    Set<Map.Entry<K, Iterator<V>>> returnSet = new HashSet<>();
    for (Map.Entry<K, List<V>> e : kvs.entrySet()) {
      returnSet.add(new AbstractMap.SimpleEntry<>(e.getKey(), e.getValue().iterator()));
    }
    return returnSet;
  }

  @Override
  public boolean contains(K key) {
    return false;
  }

  @Override
  public void close() {
    for (Subscriber<? super Map.Entry<K, Iterator<V>>> s : subscribers) {
      s.onCompleted();
    }
  }

  @Override
  public String getName() {
    return configuration.getName();
  }

  @Override
  public void call(Subscriber<? super Map.Entry<K, Iterator<V>>> subscriber) {
    subscribers.add(subscriber);
  }
}
