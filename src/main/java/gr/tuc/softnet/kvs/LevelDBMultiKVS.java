package gr.tuc.softnet.kvs;

import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.iq80.leveldb.CompressionType;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBFactory;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.WriteBatch;
import org.iq80.leveldb.WriteOptions;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import rx.Observable;
import rx.Subscriber;

import static org.fusesource.leveldbjni.JniDBFactory.bytes;
import static org.fusesource.leveldbjni.JniDBFactory.factory;

/**
 * Created by ap0n on 7/4/2016.
 */
public class LevelDBMultiKVS<K extends WritableComparable, V extends Writable>
    implements IntermediateKeyValueStore<K, V> {

  private KVSConfiguration configuration;
  private WriteOptions writeOptions;
  private DB keysDB;
  private DB dataDB;
  private File baseDirFile;
  private File keydbFile;
  private File datadbFile;
  private Options options;
  private WriteBatch batch;
  private DBFactory dbfactory;
  private Class<V> valueClass;
  private Class<K> keyClass;
  private int batchSize = 50000;
  private int batchCount = 0;
  private int size = 0;

  public LevelDBMultiKVS(KVSConfiguration configuration) {
    this.configuration = configuration;
    baseDirFile = new File(configuration.getBaseDir());
    if (baseDirFile.exists() && baseDirFile.isDirectory()) {
      for (File f : baseDirFile.listFiles()) {
        f.delete();
      }
      baseDirFile.delete();
    } else if (baseDirFile.exists()) {
      baseDirFile.delete();
    }
    baseDirFile.mkdirs();
    keydbFile = new File(baseDirFile.toString() + "/" + configuration.getName() + ".keydb");
    datadbFile = new File(baseDirFile.toString() + "/" + configuration.getName() + ".datadb");
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
      keysDB = dbfactory.open(keydbFile, options.verifyChecksums(true));
      dataDB = dbfactory.open(datadbFile, options);
      writeOptions = new WriteOptions();
      writeOptions.sync(false);

      batch = dataDB.createWriteBatch();
    } catch (IOException e) {
      e.printStackTrace();
    }
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
  }

  @Override
  public void append(K key, V value) {
    try {
      size++;
      ByteArrayDataOutput keyBytes = ByteStreams.newDataOutput();
      ByteArrayDataOutput valueBytes = ByteStreams.newDataOutput();
      ByteArrayDataOutput keyWrapper = ByteStreams.newDataOutput();
      key.write(keyBytes);
      value.write(valueBytes);
      byte[] count = keysDB.get(keyBytes.toByteArray());
      Integer counter;
      if (count == null) {
        counter = 0;
      } else {
        counter = Integer.parseInt(new String(count));
        counter += 1;
      }
      KeyWrapper<K> wrapper = new KeyWrapper<>(key, counter);
      wrapper.write(keyWrapper);
      byte[] keyvalue = bytes(counter.toString());
      keysDB.put(keyBytes.toByteArray(), keyvalue, writeOptions);
      batch.put(keyWrapper.toByteArray(), valueBytes.toByteArray());
      batchCount++;
      if (batchCount >= batchSize) {
        try {
          dataDB.write(batch, writeOptions);
          batch.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
        batch = dataDB.createWriteBatch();
        batchCount = 0;
      }

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public void flush() {
    if (dataDB != null) {
      if (batch != null) {
        dataDB.write(batch);
        try {
          batch.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
        batch = dataDB.createWriteBatch();
      }
    }
  }

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
    flush();
    return new LevelDBMultiKVSIterator<>(dataDB, keysDB, keyClass, valueClass);
  }

  @Override
  public boolean contains(K key) {
    ByteArrayDataOutput keyBytes = ByteStreams.newDataOutput();
    try {
      key.write(keyBytes);
    } catch (IOException e) {
      e.printStackTrace();
    }
    byte[] count = keysDB.get(keyBytes.toByteArray());
    return count != null;
  }

  @Override
  public void close() {
    if (keysDB != null) {
      try {
        keysDB.close();
        dbfactory.destroy(keydbFile, new Options());
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    keysDB = null;
    if (dataDB != null) {
      try {
        if (batch != null) {
          batch.close();
        }

        dataDB.close();
        dbfactory.destroy(datadbFile, new Options());
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    dataDB = null;

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
