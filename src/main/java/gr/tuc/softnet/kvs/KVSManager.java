package gr.tuc.softnet.kvs;

import gr.tuc.softnet.core.MCInitializable;
import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Created by vagvaz on 10/02/16.
 */
public interface KVSManager extends MCInitializable {

   < K extends WritableComparable, V extends Writable> KeyValueStore<K,V> createKVS(String name, Configuration configuration);
    boolean destroyKVS(String name);
    < K extends WritableComparable, V extends Writable> KeyValueStore<K,V> getKVS(String name);
    < K extends WritableComparable, V extends Writable> KVSProxy<K,V> getKVSProxy(String name);


  KVSConfiguration getKVSConfiguration(String name);

  KVSConfiguration bootstrapKVS(KVSConfiguration kvsConfiguration);

  Class<? extends WritableComparable> getKeyClass(KeyValueStore store);
  Class<? extends Writable> getValueClass(KeyValueStore store);

  KVSConfiguration remoteCreateKVS(String name, KVSConfiguration configuraiton);
}
