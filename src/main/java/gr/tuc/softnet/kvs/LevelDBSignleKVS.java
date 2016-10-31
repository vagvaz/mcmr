package gr.tuc.softnet.kvs;

import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.ReflectionUtils;
import org.iq80.leveldb.CompressionType;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBFactory;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.WriteBatch;
import org.iq80.leveldb.WriteOptions;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import rx.Observable;
import rx.Subscriber;

import static org.fusesource.leveldbjni.JniDBFactory.factory;

/**
 * Created by ap0n on 5/4/2016.
 */
public class LevelDBSignleKVS<K extends WritableComparable, V extends Writable>
    implements KeyValueStore<K, V> {

  private KVSConfiguration configuration;
  private boolean iteratorReturned;
  private DB dataDB;
  private WriteBatch batch;
  private File datadbFile;
  private Options options;
  private WriteOptions writeOptions;
  private DBFactory dbfactory;
  private int batchSize;
  private int batchCount;
  private Class<K> keyClass;
  private Class<V> valueClass;
  private Configuration hadoopConf;
  private AtomicInteger size;

  public LevelDBSignleKVS(KVSConfiguration configuration) {
    this.configuration = configuration;
    batchSize = 50000;
    batchCount = 0;
    hadoopConf = new Configuration();
    size = new AtomicInteger(0);
    iteratorReturned = false;

    File baseDirFile = new File(configuration.getBaseDir());
    if (baseDirFile.exists() && baseDirFile.isDirectory()) {
      for (File f : baseDirFile.listFiles()) {
        f.delete();
      }
      baseDirFile.delete();
    } else if (baseDirFile.exists()) {
      baseDirFile.delete();
    }
    baseDirFile.mkdirs();
    datadbFile = new File(baseDirFile.toString() + "/" + configuration.getName()+".datadb");
    options = new Options();
    options.writeBufferSize(50 * 1024 * 1024);
    options.createIfMissing(true);
    //        options.blockSize(LQPConfiguration.getInstance().conf()
    //            .getInt("leads.processor.infinispan.leveldb.blocksize", 16)*1024*1024);
    //        options.cacheSize(LQPConfiguration.getInstance().conf()
    //            .getInt("leads.processor.infinispan.leveldb.cachesize", 256)*1024*1024);
    options.blockSize(4 * 1024);
    options.compressionType(CompressionType.SNAPPY);
    options.cacheSize(64 * 1024 * 1024);

    dbfactory = factory;
    //        JniDBFactory.pushMemoryPool(128*1024*1024);

    try {
      dataDB = dbfactory.open(datadbFile, options.verifyChecksums(true));
      writeOptions = new WriteOptions();
      writeOptions.sync(false);
      batch = dataDB.createWriteBatch();
    } catch (IOException e) {
      e.printStackTrace();
    }

    valueClass = null;
    try {
      valueClass = (Class<V>)configuration.getValueClass();
    } catch (Exception e) {
      e.printStackTrace();
    }

    keyClass = null;
    try {
      keyClass = (Class<K>)configuration.getKeyClass();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public void flush() {
    if (batchCount > 0) {
      dataDB.write(batch, writeOptions);
      batchCount = 0;
      try {
        batch.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
      batch = dataDB.createWriteBatch();
    }
  }

  @Override
  public void put(K key, V value) throws Exception {
    if (iteratorReturned) {
//      throw new Exception("Trying to write after has returned iterator");
      System.err.println("Trying to write after has returned iterator");
    }

    ByteArrayDataOutput keyBytes = ByteStreams.newDataOutput();
    ByteArrayDataOutput valueBytes = ByteStreams.newDataOutput();
    try {
      key.write(keyBytes);
      value.write(valueBytes);
    } catch (IOException e) {
      e.printStackTrace();
    }

    batch.put(keyBytes.toByteArray(), valueBytes.toByteArray());
    if (++batchCount >= batchSize) {
      dataDB.write(batch, writeOptions);
      try {
        batch.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
      batch = dataDB.createWriteBatch();
      batchCount = 0;
    }
    size.incrementAndGet();  // TODO(ap0n): what about double entries?
  }

  @Override
  public V get(K key) {
    flush();

    ByteArrayDataOutput output = ByteStreams.newDataOutput();
    try {
      key.write(output);
    } catch (IOException e) {
      e.printStackTrace();
    }

    byte[] value = dataDB.get(output.toByteArray());
    if (value == null) {
      return null;
    }
    ByteArrayDataInput input = ByteStreams.newDataInput(value);
    V result = ReflectionUtils.newInstance(valueClass, hadoopConf);
    try {
      result.readFields(input);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return result;
  }

  @Override
  public int size() {
    return size.get();
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
    iteratorReturned = true;
    flush();
    return new LevelDBSingleKVSIterator<>(dataDB, keyClass, valueClass);
  }

  @Override
  public boolean contains(K key) {
    return get(key) != null;
  }

  @Override
  public void close() {
    try {
      dataDB.close();
      dbfactory.destroy(datadbFile, new Options());
    } catch (IOException e) {
      e.printStackTrace();
    }
    dataDB = null;
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
