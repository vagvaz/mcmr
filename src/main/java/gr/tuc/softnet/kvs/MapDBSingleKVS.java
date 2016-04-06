package gr.tuc.softnet.kvs;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;

import java.io.File;
import java.util.Iterator;
import java.util.Map;

import gr.tuc.softnet.core.WritableComparableSerializer;
import gr.tuc.softnet.core.WritableSerializer;
import rx.Observable;
import rx.Subscriber;

/**
 * Created by ap0n on 5/4/2016.
 */
public class MapDBSingleKVS<K extends WritableComparable, V extends Writable>
    implements KeyValueStore<K, V> {

  private KVSConfiguration configuration;
  private DB db;
  private BTreeMap<K, V> dataDb;
  private boolean iteratorReturned;
  private Class<V> valueClass;
  private Class<K> keyClass;

  public MapDBSingleKVS(KVSConfiguration configuration) {
    this.configuration = configuration;
    iteratorReturned = false;
    db = DBMaker.newTempFileDB()
        .compressionEnable()
        .mmapFileEnableIfSupported()
        .transactionDisable()
        .closeOnJvmShutdown()
        .deleteFilesAfterClose()
        .asyncWriteEnable()
        .make();
    File baseDirFile = new File(configuration.getBaseDir());
    if (baseDirFile.exists() && baseDirFile.isDirectory()) {
      for (File f : baseDirFile.listFiles()) {
        f.delete();
      }
      baseDirFile.delete();
    } else if (baseDirFile.exists()) {
      baseDirFile.delete();
    }

    keyClass = null;
    try {
      keyClass = (Class<K>) configuration.getKeyClass();
    } catch (Exception e) {
      e.printStackTrace();
    }
    valueClass = null;
    try {
      valueClass = (Class<V>) configuration.getValueClass();
    } catch (Exception e) {
      e.printStackTrace();
    }

    baseDirFile.getParentFile().mkdirs();
    dataDb = db.createTreeMap(baseDirFile.toString() + "/" + configuration.getName() + ".datadb")
        .nodeSize(100)
        .keySerializer(new WritableComparableSerializer<K>(keyClass))
        .valueSerializer(new WritableSerializer<V>(valueClass))
        .makeOrGet();
  }

  @Override
  public void flush() {}

  @Override
  public void put(K key, V value) throws Exception {
    if (iteratorReturned) {
      throw new Exception("Trying to write after has returned iterator");
    }
    dataDb.put(key, value);
  }

  @Override
  public V get(K key) {
    return dataDb.get(key);
  }

  @Override
  public int size() {
    return dataDb.size();
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
    // For batch this will be called only once! If someone tries to write after this is called,
    // throw an exception.
    iteratorReturned = true;
    return dataDb.entrySet();
  }

  @Override
  public boolean contains(K key) {
    return dataDb.containsKey(key);
  }

  @Override
  public void close() {
    dataDb.close();
  }

  @Override
  public String getName() {
    return configuration.getName();
  }

  @Override
  public void call(Subscriber<? super Map.Entry<K, V>> subscriber) {
    Observable.from(this.iterator()).subscribe(subscriber);
  }
}
