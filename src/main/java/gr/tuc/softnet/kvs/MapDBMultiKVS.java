package gr.tuc.softnet.kvs;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.File;
import java.util.Iterator;
import java.util.Map;

import gr.tuc.softnet.core.WritableComparableSerializer;
import gr.tuc.softnet.core.WritableSerializer;
import rx.Observable;
import rx.Subscriber;

/**
 * Created by ap0n on 6/4/2016.
 */
public class MapDBMultiKVS<K extends WritableComparable, V extends Writable>
    implements IntermediateKeyValueStore<K, V> {

  private KVSConfiguration configuration;
  private DB theDb;
  BTreeMap<K, Integer> keysDB;
  BTreeMap<KeyWrapper<K>, V> dataDB;
  private File baseDirFile;
  private File keydbFile;
  private File datadbFile;
  private Class<V> valueClass;
  private Class<K> keyClass;
  private int size;
  private boolean closed;

  public MapDBMultiKVS(KVSConfiguration configuration) {
    closed = false;
    this.configuration = configuration;
    size = 0;
    baseDirFile = new File(configuration.getBaseDir());
    if (baseDirFile.exists() && baseDirFile.isDirectory()) {
      for (File f : baseDirFile.listFiles()) {
        f.delete();
      }
      baseDirFile.delete();
    } else if (baseDirFile.exists()) {
      baseDirFile.delete();
    }
    baseDirFile.getParentFile().mkdirs();
    keydbFile = new File(baseDirFile.toString() + "/" + configuration.getName() + ".keydb");
    datadbFile = new File(baseDirFile.toString() + "/" + configuration.getName() + ".datadb");

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

    try {
      theDb = DBMaker.newTempFileDB().compressionEnable().mmapFileEnableIfSupported()
          .transactionDisable()
          .closeOnJvmShutdown()
          .deleteFilesAfterClose()
          .asyncWriteEnable()
          .make();
      KeyWrapper tmpWrapper = new KeyWrapper(keyClass);
      keysDB = theDb.createTreeMap(keydbFile.getName())
          .keySerializer(new WritableComparableSerializer<K>(keyClass)).makeOrGet();
      dataDB = theDb.createTreeMap(datadbFile.getName()).nodeSize(100)
          .keySerializer(new WritableComparableSerializer<>(tmpWrapper))
          .valueSerializer(new WritableSerializer<V>(valueClass)).makeOrGet();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public void append(K key, V value) {
    try {
      Integer counter = keysDB.get((key));
      if (counter == null) {
        counter = 0;
      } else {
        counter += 1;
      }
      keysDB.put(key, counter);
      KeyWrapper wrapper = new KeyWrapper(key, counter);
      dataDB.put(wrapper, value);
      size++;
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public void flush() {}

  @Override
  public void put(K key, Iterator<V> value) throws Exception {
    throw new NotImplementedException();
  }

  @Override
  public Iterator<V> get(K key) {
    throw new NotImplementedException();
  }

  @Override
  public int size() {
    return size;
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
    return new MapDBMultiKVSIterator<>(dataDB, keysDB);
  }

  @Override
  public boolean contains(K key) {
    return keysDB.containsKey(key);
  }

  @Override
  public void close() {
    if (closed) {
      return;
    }

    closed = true;

//    dataDB.close();  // TODO(ap0n): Check out if these must be closed as well
//    keysDB.close();
    theDb.close();
    if (baseDirFile.exists()) {
      baseDirFile.delete();
    }
  }

  @Override
  public String getName() {
    return configuration.getName();
  }

  @Override
  public void call(Subscriber<? super Map.Entry<K, Iterator<V>>> subscriber) {
    Observable.from(this.iterator()).subscribe(subscriber);
  }
}
