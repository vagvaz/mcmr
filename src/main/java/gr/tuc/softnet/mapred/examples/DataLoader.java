package gr.tuc.softnet.mapred.examples;

import com.google.inject.Injector;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.util.Map;

import gr.tuc.softnet.core.NodeManager;
import gr.tuc.softnet.core.StringConstants;
import gr.tuc.softnet.kvs.KVSConfiguration;
import gr.tuc.softnet.kvs.KVSManager;
import gr.tuc.softnet.kvs.KeyValueStore;

/**
 * Created by ap0n on 19/5/2016.
 */
public class DataLoader<K extends WritableComparable, V extends Writable> {

  private String kvsName;
  private KeyValueStore<K, V> kvs;
  private KVSConfiguration kvsConfiguration;
  private NodeManager nodeManager;
  private Class<K> keyClass;
  private Class<V> valueClass;

  public DataLoader(String kvsName, NodeManager nodeManager, Injector injector, Class<K> keyClass,
                    Class<V> valueClass) {
    this.kvsName = kvsName;
    this.kvsConfiguration = new KVSConfiguration();
    this.keyClass = keyClass;
    this.valueClass = valueClass;
    this.nodeManager = nodeManager;

    KVSManager kvsManager = injector.getInstance(KVSManager.class);

    kvsConfiguration = new KVSConfiguration(kvsName + "Configuration");  // TODO: Is the name ok?
    kvsConfiguration.setCacheType(StringConstants.PIPELINE);
    kvsConfiguration.setMaterialized(false);
    kvsConfiguration.setKeyClass(keyClass);
    kvsConfiguration.setValueClass(valueClass);
    kvs = kvsManager.createKVS(kvsName, kvsConfiguration);
  }

  public void load(K key, V value) throws Exception {
    kvs.put(key, value);
  }

  public void load(Map<K, V> data) throws Exception {
    for (Map.Entry<K, V> e : data.entrySet()) {
      load(e.getKey(), e.getValue());
    }
  }
}
