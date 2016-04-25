package gr.tuc.softnet.kvs;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Created by vagvaz on 22/02/16.
 */
public class KVSWrapper<K extends WritableComparable,V extends Writable> {
    KeyValueStore<K,V> kvs;
    KVSProxy<K,V> proxy;
    KVSConfiguration configuration;

    public KVSWrapper(KeyValueStore<K, V> newKVS, KVSProxy kvsProxy,
      KVSConfiguration kvsConfiguration){
        kvs = newKVS;
        proxy = kvsProxy;
        this.configuration = kvsConfiguration;
    }


    public  KeyValueStore<K, V> getKVS() {
        return kvs;
    }

    public KVSProxy<K, V> getProxy() {
        return proxy;
    }


    public KVSConfiguration getConfiguration() {
        return configuration;
    }
}
