package gr.tuc.softnet.kvs;

/**
 * Created by vagvaz on 22/02/16.
 */
public class KVSWrapper<K,V> {
    KeyValueStore<K,V> kvs;
    KVSProxy<K,V> proxy;

    public KVSWrapper(){

    }

    public KVSWrapper(KeyValueStore<K, V> newKVS, KVSProxy<K, V> kvsProxy) {
        kvs = newKVS;
        proxy = kvsProxy;
    }

    public  KeyValueStore<K, V> getKVS() {
        return kvs;
    }

    public KVSProxy<K, V> getProxy() {
        return proxy;
    }
}
