package gr.tuc.softnet.kvs;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Created by vagvaz on 12/11/15.
 */
public class MapDBKeyIterator<K> implements Iterable<Map.Entry<K, Integer>>,Iterator<Map.Entry<K, Integer>> {
  Iterator<Map.Entry<K, Integer>> mapdbIterator;
  public MapDBKeyIterator(Set<Map.Entry<K, Integer>> entries) {
    mapdbIterator = entries.iterator();
  }

   public Iterator<Map.Entry<K, Integer>> iterator() {
    return this;
  }

   public boolean hasNext() {
    return mapdbIterator.hasNext();
  }

   public Map.Entry<K, Integer> next() {
    Map.Entry<K,Integer> entry = mapdbIterator.next();
     return entry;
//    return new AbstractMap.SimpleEntry<K, Integer>(entry.getKey().substring(0,entry.getKey().lastIndexOf("{}")),entry.getValue());
  }

   public void remove() {

  }
}
